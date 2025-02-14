
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BOKTYP': 'varchar(max)',
 'BOKTYP_ID': 'varchar(max)',
 'BOKTYP_ID_TEXT': 'varchar(max)',
 'BOKTYP_NR': 'varchar(max)',
 'BOKTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BOKTYP AS VARCHAR(MAX)) AS BOKTYP,
CAST(BOKTYP_ID AS VARCHAR(MAX)) AS BOKTYP_ID,
CAST(BOKTYP_ID_TEXT AS VARCHAR(MAX)) AS BOKTYP_ID_TEXT,
CAST(BOKTYP_NR AS VARCHAR(MAX)) AS BOKTYP_NR,
CAST(BOKTYP_TEXT AS VARCHAR(MAX)) AS BOKTYP_TEXT FROM utdata.utdata295.RK_DIM_BOKTYP"""
    return pipe(query=query)
