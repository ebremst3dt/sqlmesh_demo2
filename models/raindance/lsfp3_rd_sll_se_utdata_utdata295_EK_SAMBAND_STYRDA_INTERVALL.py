
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'INTRADNUMMER': 'varchar(max)',
 'RADNUMMER': 'varchar(max)',
 'STYRD_ID': 'varchar(max)',
 'STYRD_NR': 'varchar(max)',
 'STYRT_INTERVALL': 'varchar(max)',
 'STYRT_INTERVALL2': 'varchar(max)',
 'STYRT_OBJEKT_FOM': 'varchar(max)',
 'STYRT_OBJEKT_TOM': 'varchar(max)'},
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
CAST(INTRADNUMMER AS VARCHAR(MAX)) AS INTRADNUMMER,
CAST(RADNUMMER AS VARCHAR(MAX)) AS RADNUMMER,
CAST(STYRD_ID AS VARCHAR(MAX)) AS STYRD_ID,
CAST(STYRD_NR AS VARCHAR(MAX)) AS STYRD_NR,
CAST(STYRT_INTERVALL AS VARCHAR(MAX)) AS STYRT_INTERVALL,
CAST(STYRT_INTERVALL2 AS VARCHAR(MAX)) AS STYRT_INTERVALL2,
CAST(STYRT_OBJEKT_FOM AS VARCHAR(MAX)) AS STYRT_OBJEKT_FOM,
CAST(STYRT_OBJEKT_TOM AS VARCHAR(MAX)) AS STYRT_OBJEKT_TOM FROM utdata.utdata295.EK_SAMBAND_STYRDA_INTERVALL"""
    return pipe(query=query)
