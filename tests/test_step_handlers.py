"""step_handlers.py is where every error the diagnostic agent has to reason
about originates: IngestionStepError, ValidationStepError,
TransformationStepError, LoadStepError. This suite exercises each step's
success path and its reachable failure modes directly against the
pandas/file-system logic, with no DB and no network beyond a monkeypatched
fetch_data(). These are also the failure shapes used later to build the
diagnostic agent's labeled eval cases, so getting the exact exception
messages right here matters beyond just coverage.
"""
import pandas as pd
import pytest

from worker.app import step_handlers as sh
from worker.app.exceptions import (
    IngestionStepError,
    LoadStepError,
    TransformationStepError,
    ValidationStepError,
)


# ---------------------------------------------------------------------------
# run_ingestion
# ---------------------------------------------------------------------------

async def test_ingestion_requires_a_source_url():
    with pytest.raises(IngestionStepError, match="No source_url"):
        await sh.run_ingestion({}, {"run_id": 1})


async def test_ingestion_wraps_fetch_failures(monkeypatch):
    async def broken_fetch(url):
        raise ConnectionError("connection refused")

    monkeypatch.setattr(sh, "fetch_data", broken_fetch)

    with pytest.raises(IngestionStepError, match="Failed to fetch"):
        await sh.run_ingestion({"source_url": "https://example.com/data.csv"}, {"run_id": 1})


async def test_ingestion_saves_the_fetched_file_and_records_it_in_run_context(monkeypatch, tmp_path):
    async def fake_fetch(url):
        return b"a,b\n1,2\n"

    monkeypatch.setattr(sh, "fetch_data", fake_fetch)
    monkeypatch.chdir(tmp_path)

    run_context = {"run_id": 42}
    await sh.run_ingestion({"source_url": "https://example.com/data.csv"}, run_context)

    assert "ingestion" in run_context
    saved_path = run_context["ingestion"]["file_path"]
    assert (tmp_path / saved_path).exists()
    assert (tmp_path / saved_path).read_bytes() == b"a,b\n1,2\n"


# ---------------------------------------------------------------------------
# run_validation
# ---------------------------------------------------------------------------

async def test_validation_requires_ingestion_output():
    with pytest.raises(ValidationStepError, match="requires ingestion output"):
        await sh.run_validation({}, {})


async def test_validation_requires_the_ingested_file_to_exist(tmp_path):
    missing = tmp_path / "does_not_exist.csv"
    with pytest.raises(ValidationStepError, match="File not found"):
        await sh.run_validation({}, {"ingestion": {"file_path": str(missing)}})


async def test_validation_flags_missing_required_columns(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n")

    with pytest.raises(ValidationStepError, match="Missing required columns"):
        await sh.run_validation(
            {"required_columns": ["a", "c"]},
            {"ingestion": {"file_path": str(csv_path)}},
        )


async def test_validation_flags_null_values_in_required_columns(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,\n2,3\n")

    with pytest.raises(ValidationStepError, match="contains null values"):
        await sh.run_validation(
            {"non_null_columns": ["b"]},
            {"ingestion": {"file_path": str(csv_path)}},
        )


async def test_validation_flags_too_few_rows(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n")

    with pytest.raises(ValidationStepError, match="expected at least"):
        await sh.run_validation(
            {"min_rows": 5},
            {"ingestion": {"file_path": str(csv_path)}},
        )


async def test_validation_passes_and_records_row_and_column_counts(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n")

    run_context = {"ingestion": {"file_path": str(csv_path)}}
    await sh.run_validation({"required_columns": ["a", "b"], "min_rows": 1}, run_context)

    assert run_context["validation"]["status"] == "passed"
    assert run_context["validation"]["row_count"] == 2
    assert run_context["validation"]["column_count"] == 2


# ---------------------------------------------------------------------------
# run_transformation
# ---------------------------------------------------------------------------

async def test_transformation_requires_validated_file_path():
    with pytest.raises(TransformationStepError, match="requires validated_file_path"):
        await sh.run_transformation({}, {})


async def test_transformation_rejects_an_unknown_filter_column(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n")

    with pytest.raises(TransformationStepError, match="Unknown filter column"):
        await sh.run_transformation(
            {"filter_rows": [{"column": "z", "operator": ">", "value": "0"}]},
            {"validation": {"validated_file_path": str(csv_path)}},
        )


async def test_transformation_rejects_an_unsupported_operator(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n")

    with pytest.raises(TransformationStepError, match="unsupported"):
        await sh.run_transformation(
            {"filter_rows": [{"column": "a", "operator": "~=", "value": "1"}]},
            {"validation": {"validated_file_path": str(csv_path)}},
        )


async def test_transformation_rejects_a_type_mismatch_on_filter_value(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n")

    with pytest.raises(TransformationStepError, match="Type mismatch"):
        await sh.run_transformation(
            {"filter_rows": [{"column": "a", "operator": ">", "value": "not-a-number"}]},
            {"validation": {"validated_file_path": str(csv_path)}},
        )


async def test_transformation_renames_filters_and_drops_columns(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b,c\n1,2,x\n5,6,y\n")

    run_context = {"validation": {"validated_file_path": str(csv_path)}}
    await sh.run_transformation(
        {
            "rename_columns": {"a": "id"},
            "filter_rows": [{"column": "id", "operator": ">", "value": "2"}],
            "drop_columns": ["c"],
        },
        run_context,
    )

    out_path = run_context["transformation"]["transformed_file_path"]
    df = pd.read_csv(out_path)
    assert list(df.columns) == ["id", "b"]
    assert df["id"].tolist() == [5]


# ---------------------------------------------------------------------------
# run_load
# ---------------------------------------------------------------------------

async def test_load_requires_transformed_file_path():
    with pytest.raises(LoadStepError, match="requires transformed_file_path"):
        await sh.run_load({}, {})


async def test_load_wraps_data_warehouse_write_failures(monkeypatch, tmp_path):
    csv_path = tmp_path / "transformed.csv"
    csv_path.write_text("a,b\n1,2\n")

    def broken_to_sql(self, *args, **kwargs):
        raise RuntimeError("relation does not exist")

    monkeypatch.setattr(pd.DataFrame, "to_sql", broken_to_sql)

    with pytest.raises(LoadStepError, match="Failed to write table"):
        await sh.run_load(
            {},
            {"transformation": {"transformed_file_path": str(csv_path)}, "tenant_id": 1, "pipeline_id": 2},
        )


async def test_load_succeeds_and_records_table_name(monkeypatch, tmp_path):
    csv_path = tmp_path / "transformed.csv"
    csv_path.write_text("a,b\n1,2\n")

    calls = []

    def fake_to_sql(self, name, con, if_exists, index):
        calls.append(name)

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    run_context = {"transformation": {"transformed_file_path": str(csv_path)}, "tenant_id": 1, "pipeline_id": 2}
    await sh.run_load({}, run_context)

    assert calls == ["tenant_1_pipeline_2"]
    assert run_context["load"]["data_warehouse_table_name"] == "tenant_1_pipeline_2"
    assert run_context["load"]["row_count"] == 1
