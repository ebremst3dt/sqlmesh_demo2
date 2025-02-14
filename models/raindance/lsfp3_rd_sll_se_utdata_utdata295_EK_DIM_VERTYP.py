
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DELSYSTEM': 'varchar(max)',
 'DELSYSTEM_TEXT': 'varchar(max)',
 'VERTYP': 'varchar(max)',
 'VERTYP_PASSIV': 'varchar(max)',
 'VERTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(DELSYSTEM AS VARCHAR(MAX)) AS DELSYSTEM,
CAST(DELSYSTEM_TEXT AS VARCHAR(MAX)) AS DELSYSTEM_TEXT,
CAST(VERTYP AS VARCHAR(MAX)) AS VERTYP,
CAST(VERTYP_PASSIV AS VARCHAR(MAX)) AS VERTYP_PASSIV,
CAST(VERTYP_TEXT AS VARCHAR(MAX)) AS VERTYP_TEXT FROM utdata.utdata295.EK_DIM_VERTYP"""
    return pipe(query=query)
