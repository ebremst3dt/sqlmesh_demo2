
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'PERSONALKAT_ID': 'varchar(max)',
 'PERSONALKAT_ID_TEXT': 'varchar(max)',
 'PERSONALKAT_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(PERSONALKAT_ID AS VARCHAR(MAX)) AS PERSONALKAT_ID,
CAST(PERSONALKAT_ID_TEXT AS VARCHAR(MAX)) AS PERSONALKAT_ID_TEXT,
CAST(PERSONALKAT_TEXT AS VARCHAR(MAX)) AS PERSONALKAT_TEXT FROM utdata.utdata295.EK_DIM_PERSONALKAT"""
    return pipe(query=query)
