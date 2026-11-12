"""The configuration check that stops a process at startup instead of at the first request that needs a setting."""
import pytest

from shared import config

GOOD = {
    "DATABASE_URL": "postgresql+asyncpg://u:p@db/x",
    "DATABASE_URL_SYNC": "postgresql://u:p@db/x",
    "REDIS_URL": "redis://redis:6379/0",
    "DATA_WAREHOUSE_URL": "postgresql://u:p@db/dw",
    "API_KEY_SECRET": "a" * 24,
    "ADMIN_SECRET_KEY": "b" * 24,
}


def test_a_complete_configuration_has_no_problems():
    assert config.settings_problems(GOOD) == []


@pytest.mark.parametrize("name", sorted(GOOD))
def test_every_required_setting_is_named_when_missing(name):
    env = {key: value for key, value in GOOD.items() if key != name}
    assert config.settings_problems(env) == [f"{name} is not set"]


def test_a_blank_secret_counts_as_missing():
    assert config.settings_problems({**GOOD, "ADMIN_SECRET_KEY": "   "}) == ["ADMIN_SECRET_KEY is not set"]


def test_all_problems_are_reported_together():
    assert len(config.settings_problems({})) == len(GOOD)


def test_short_secrets_are_allowed_outside_production():
    env = {**GOOD, "API_KEY_SECRET": "short", "ADMIN_SECRET_KEY": "short2", "ENV": "dev"}
    assert config.settings_problems(env) == []


def test_production_needs_long_secrets():
    env = {**GOOD, "ENV": "production", "API_KEY_SECRET": "short", "ADMIN_SECRET_KEY": "b" * 24}
    assert config.settings_problems(env) == ["API_KEY_SECRET must be at least 16 characters when ENV=production"]


def test_production_needs_two_different_secrets():
    env = {**GOOD, "ENV": "production", "API_KEY_SECRET": "c" * 24, "ADMIN_SECRET_KEY": "c" * 24}
    assert config.settings_problems(env) == ["API_KEY_SECRET and ADMIN_SECRET_KEY must differ when ENV=production"]


def test_a_missing_secret_in_production_is_one_problem_not_two():
    env = {key: value for key, value in GOOD.items() if key != "API_KEY_SECRET"}
    env["ENV"] = "production"
    assert config.settings_problems(env) == ["API_KEY_SECRET is not set"]


def test_validate_raises_with_every_problem_in_the_message(monkeypatch):
    for name in GOOD:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError) as error:
        config.validate_settings()
    for name in GOOD:
        assert f"{name} is not set" in str(error.value)


def test_validate_passes_for_a_good_environment(monkeypatch):
    for name, value in GOOD.items():
        monkeypatch.setenv(name, value)
    config.validate_settings()
