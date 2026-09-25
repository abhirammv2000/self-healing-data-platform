import httpx
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from worker.app.exceptions import IngestionStepError, ValidationStepError, TransformationStepError, LoadStepError
import operator
from shared.db import dw_engine
from shared.observability import get_logger

log=get_logger(__name__)

#step execution functions
async def run_ingestion(config, run_context):
    log.info("ingestion_started", run_id=run_context.get("run_id"), config=config)
    
    run_id=run_context["run_id"]
    source_url=config.get("source_url")
    if not source_url:
        raise IngestionStepError(f"No source_url parameter. Ingestion step requires a source_url in config")

    try:
        raw=await fetch_data(source_url)
    except Exception as e:
        raise IngestionStepError(f"Failed to fetch from {source_url}: {e}")

    #save in a local directory for now
    run_dir=Path("data")/"runs"/str(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    #try to extract original filename from URL or save it as 'file'
    parsed_url=urlparse(source_url)
    original_name=Path(parsed_url.path).name or "file"

    #add timestamp to the filename
    timestamp=datetime.now().replace(tzinfo=None).strftime("%Y%m%d_%H%M%S")
    filename=f"{timestamp}_{original_name}"
    file_path=run_dir/filename

    # save file
    file_path.write_bytes(raw)

    run_context["ingestion"]={"file_path":str(file_path), "source_url": source_url, "ingested_at": timestamp}
    log.info("ingestion_succeeded", run_id=run_context.get("run_id"), file_path=str(file_path))

async def run_validation(config, run_context):
    log.info("validation_started", run_id=run_context.get("run_id"), config=config)

    ingestion_data=run_context.get("ingestion") #get the context from ingestion step
    if not ingestion_data or "file_path" not in ingestion_data:
        raise ValidationStepError("Validation step requires ingestion output file_path in run_context")

    file_path=Path(ingestion_data["file_path"]) #get the file path
    if not file_path.exists():
        raise ValidationStepError(f"File not found: {file_path}")
    
    try: #try reading the file
        df=pd.read_csv(file_path)
    except Exception as e:
        raise ValidationStepError(f"Failed to read CSV file: {e}")
    
    #collect all failed checks in a list, and then finally raise the validation error with the list of errors
    errors=[]

    #check for the required columns
    required_columns=config.get("required_columns", [])
    missing_columns=[col for col in required_columns if col not in df.columns]
    if missing_columns:
        errors.append(f"Missing required columns: {missing_columns}")
    
    #check the columns which have to be non null
    non_null_columns=config.get("non_null_columns", [])
    for col in non_null_columns:
        if col in df.columns and df[col].isnull().any():
            errors.append(f"Column '{col}' contains null values")
    
    #check for the min row count
    min_rows=config.get("min_rows")
    if min_rows is not None and len(df) < min_rows:
        errors.append(f"CSV has {len(df)} rows, expected at least {min_rows}")
    
    if errors:
        raise ValidationStepError("Validation failed: " + "; ".join(errors))
    
    run_context["validation"]={"status": "passed", "validated_file_path":str(file_path), "row_count":len(df), "column_count":len(df.columns)}
    log.info("validation_succeeded", run_id=run_context.get("run_id"), row_count=len(df), column_count=len(df.columns))

async def run_transformation(config, run_context):
    log.info("transformation_started", run_id=run_context.get("run_id"), config=config)

    validation_data=run_context.get("validation")
    if not validation_data or "validated_file_path" not in validation_data:
        raise TransformationStepError("Transformation step requires validated_file_path in run_context")
    
    file_path=Path(validation_data["validated_file_path"])
    
    try: #try reading the file
        df=pd.read_csv(file_path)
    except Exception as e:
        raise TransformationStepError(f"Failed to read CSV file: {e}")

    #rename first, filter next, and then drop
    rename_columns=config.get("rename_columns",{})
    if rename_columns:
        try:
            df.rename(columns=rename_columns, inplace=True)
        except Exception as e:
            raise TransformationStepError(f"Unable to rename columns: {e}")
    
    filter_rows=config.get("filter_rows",[])
    if filter_rows:
        for filter in filter_rows:
            column=filter["column"]
            op_str=filter["operator"]
            value=filter["value"]

            if column not in df.columns: #if the column doesn't exist in the df
                raise TransformationStepError(f"Unknown filter column: '{column}'")
            
            if op_str not in ops: #check if the op is supported or not
                raise TransformationStepError(f"Unable to filter rows: operation '{op_str}' is unsupported")
            
            target_dtype = df[column].dtype

            #cast the filter value to match the column's type
            try:
                #for numeric types (int, float), this converts the string/input value
                #for object/string types, it ensures it's a string
                casted_value=target_dtype.type(value)
            except (ValueError, TypeError) as e:
                raise TransformationStepError(
                    f"Type mismatch: Cannot cast filter value '{value}' to {target_dtype} for column '{column}'"
                )

            try:#get the corresponding operator function and apply it to the df
                op_func=ops[op_str]
                df=df[op_func(df[column],casted_value)]
            except Exception as e:
                raise TransformationStepError(f"Unable to apply filter {column} {op_str} {value}: {e}")

    drop_columns=config.get("drop_columns",[])
    if drop_columns:
        try:
            df.drop(drop_columns, axis=1, inplace=True)
        except Exception as e:
            raise TransformationStepError(f"Unable to drop columns: {e}")
    
    #save the transformed file in a local directory for now
    transformed_file_path=file_path.parent/f"{file_path.stem}_transformed{file_path.suffix}"
    df.to_csv(transformed_file_path, index=False)

    run_context["transformation"]={"input_file_path": str(file_path), "transformed_file_path": str(transformed_file_path),
                                   "row_count": len(df), "column_count": len(df.columns)}
    log.info("transformation_succeeded", run_id=run_context.get("run_id"), row_count=len(df), column_count=len(df.columns))

async def run_load(config, run_context):
    log.info("load_started", run_id=run_context.get("run_id"), config=config)

    transformation_data=run_context.get("transformation")
    if not transformation_data or "transformed_file_path" not in transformation_data:
        raise LoadStepError("Load step requires transformed_file_path in run_context")
    
    file_path=Path(transformation_data["transformed_file_path"])
    
    try: #try reading the file
        df=pd.read_csv(file_path)
    except Exception as e:
        raise LoadStepError(f"Failed to read transformed CSV file: {e}")
    
    tenant_id=run_context["tenant_id"]
    pipeline_id=run_context["pipeline_id"]

    try: #writing to the data warehouse
        table_name=f"tenant_{tenant_id}_pipeline_{pipeline_id}"
        df.to_sql(table_name, con=dw_engine, if_exists="replace", index=False)
    except Exception as e:
        raise LoadStepError(f"Failed to write table to data warehouse: {e}")

    run_context["load"]={"data_warehouse_table_name": table_name, "row_count": len(df), "column_count": len(df.columns)}
    log.info("load_succeeded", run_id=run_context.get("run_id"), table_name=table_name, row_count=len(df))

#helper functions and mappings
async def fetch_data(url: str):
    async with httpx.AsyncClient() as client:
        response=await client.get(url)
        response.raise_for_status()

        return response.content #returns bytes

step_registry={"ingestion":run_ingestion,
               "validation":run_validation,
               "transformation":run_transformation,
               "load":run_load,}

ops={">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    "<=": operator.le}