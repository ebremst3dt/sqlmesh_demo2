
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANTECKNING': 'varchar(max)',
 'ANTECKNING1': 'varchar(max)',
 'ANTECKNING10': 'varchar(max)',
 'ANTECKNING2': 'varchar(max)',
 'ANTECKNING3': 'varchar(max)',
 'ANTECKNING4': 'varchar(max)',
 'ANTECKNING5': 'varchar(max)',
 'ANTECKNING6': 'varchar(max)',
 'ANTECKNING7': 'varchar(max)',
 'ANTECKNING8': 'varchar(max)',
 'ANTECKNING9': 'varchar(max)',
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
    query = """SELECT CAST(ANTECKNING AS VARCHAR(MAX)) AS ANTECKNING,
CAST(ANTECKNING1 AS VARCHAR(MAX)) AS ANTECKNING1,
CAST(ANTECKNING10 AS VARCHAR(MAX)) AS ANTECKNING10,
CAST(ANTECKNING2 AS VARCHAR(MAX)) AS ANTECKNING2,
CAST(ANTECKNING3 AS VARCHAR(MAX)) AS ANTECKNING3,
CAST(ANTECKNING4 AS VARCHAR(MAX)) AS ANTECKNING4,
CAST(ANTECKNING5 AS VARCHAR(MAX)) AS ANTECKNING5,
CAST(ANTECKNING6 AS VARCHAR(MAX)) AS ANTECKNING6,
CAST(ANTECKNING7 AS VARCHAR(MAX)) AS ANTECKNING7,
CAST(ANTECKNING8 AS VARCHAR(MAX)) AS ANTECKNING8,
CAST(ANTECKNING9 AS VARCHAR(MAX)) AS ANTECKNING9,
CAST(DOKTYP AS VARCHAR(MAX)) AS DOKTYP,
CAST(DOKUMENTID AS VARCHAR(MAX)) AS DOKUMENTID FROM utdata.utdata295.EK_ANTECKNING"""
    return pipe(query=query)
