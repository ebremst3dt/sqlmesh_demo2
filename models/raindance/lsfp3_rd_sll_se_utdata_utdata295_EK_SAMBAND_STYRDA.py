
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'NUMERISK_VANSTER': 'varchar(max)',
 'STYRD_ID': 'varchar(max)',
 'STYRD_NR': 'varchar(max)',
 'URVAL': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(NUMERISK_VANSTER AS VARCHAR(MAX)) AS NUMERISK_VANSTER,
CAST(STYRD_ID AS VARCHAR(MAX)) AS STYRD_ID,
CAST(STYRD_NR AS VARCHAR(MAX)) AS STYRD_NR,
CAST(URVAL AS VARCHAR(MAX)) AS URVAL FROM utdata.utdata295.EK_SAMBAND_STYRDA"""
    return pipe(query=query)
