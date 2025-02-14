
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DELSYS': 'varchar(max)',
 'DELSYS_TEXT': 'varchar(max)',
 'DOKUMENTTYP': 'varchar(max)',
 'EXTERNID': 'varchar(max)',
 'EXTERNID2': 'varchar(max)',
 'EXTERNID2_ID_TEXT': 'varchar(max)',
 'EXTERNID_GRUPP': 'varchar(max)',
 'EXTERNID_ID_TEXT': 'varchar(max)',
 'EXTERNID_TEXT': 'varchar(max)',
 'NAMN2': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(DELSYS AS VARCHAR(MAX)) AS DELSYS,
CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS DELSYS_TEXT,
CAST(DOKUMENTTYP AS VARCHAR(MAX)) AS DOKUMENTTYP,
CAST(EXTERNID AS VARCHAR(MAX)) AS EXTERNID,
CAST(EXTERNID_GRUPP AS VARCHAR(MAX)) AS EXTERNID_GRUPP,
CAST(EXTERNID_ID_TEXT AS VARCHAR(MAX)) AS EXTERNID_ID_TEXT,
CAST(EXTERNID_TEXT AS VARCHAR(MAX)) AS EXTERNID_TEXT,
CAST(EXTERNID2 AS VARCHAR(MAX)) AS EXTERNID2,
CAST(EXTERNID2_ID_TEXT AS VARCHAR(MAX)) AS EXTERNID2_ID_TEXT,
CAST(NAMN2 AS VARCHAR(MAX)) AS NAMN2 FROM utdata.utdata295.EK_DIM_EXTERNID"""
    return pipe(query=query)
