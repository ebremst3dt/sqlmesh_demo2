
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'EJITABELL_ID': 'varchar(max)',
 'EJITABELL_NR': 'varchar(max)',
 'INVIA_ID': 'varchar(max)',
 'INVIA_NR': 'varchar(max)',
 'OBJTYPLANGD': 'varchar(max)',
 'OBJTYP_ID': 'varchar(max)',
 'OBJTYP_ID_TEXT': 'varchar(max)',
 'OBJTYP_NR': 'varchar(max)',
 'OBJTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(EJITABELL_ID AS VARCHAR(MAX)) AS EJITABELL_ID,
CAST(EJITABELL_NR AS VARCHAR(MAX)) AS EJITABELL_NR,
CAST(INVIA_ID AS VARCHAR(MAX)) AS INVIA_ID,
CAST(INVIA_NR AS VARCHAR(MAX)) AS INVIA_NR,
CAST(OBJTYP_ID AS VARCHAR(MAX)) AS OBJTYP_ID,
CAST(OBJTYP_ID_TEXT AS VARCHAR(MAX)) AS OBJTYP_ID_TEXT,
CAST(OBJTYP_NR AS VARCHAR(MAX)) AS OBJTYP_NR,
CAST(OBJTYP_TEXT AS VARCHAR(MAX)) AS OBJTYP_TEXT,
CAST(OBJTYPLANGD AS VARCHAR(MAX)) AS OBJTYPLANGD FROM utdata.utdata295.EK_OBJTYPER"""
    return pipe(query=query)
