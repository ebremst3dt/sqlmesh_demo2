
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BILAGA1': 'varchar(max)',
 'BILAGA10': 'varchar(max)',
 'BILAGA2': 'varchar(max)',
 'BILAGA3': 'varchar(max)',
 'BILAGA4': 'varchar(max)',
 'BILAGA5': 'varchar(max)',
 'BILAGA6': 'varchar(max)',
 'BILAGA7': 'varchar(max)',
 'BILAGA8': 'varchar(max)',
 'BILAGA9': 'varchar(max)',
 'DOKTYP': 'varchar(max)',
 'DOKUMENTID': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BILAGA1 AS VARCHAR(MAX)) AS BILAGA1,
CAST(BILAGA10 AS VARCHAR(MAX)) AS BILAGA10,
CAST(BILAGA2 AS VARCHAR(MAX)) AS BILAGA2,
CAST(BILAGA3 AS VARCHAR(MAX)) AS BILAGA3,
CAST(BILAGA4 AS VARCHAR(MAX)) AS BILAGA4,
CAST(BILAGA5 AS VARCHAR(MAX)) AS BILAGA5,
CAST(BILAGA6 AS VARCHAR(MAX)) AS BILAGA6,
CAST(BILAGA7 AS VARCHAR(MAX)) AS BILAGA7,
CAST(BILAGA8 AS VARCHAR(MAX)) AS BILAGA8,
CAST(BILAGA9 AS VARCHAR(MAX)) AS BILAGA9,
CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID FROM utdata.utdata295.EK_BILAGA"""
    return pipe(query=query)
