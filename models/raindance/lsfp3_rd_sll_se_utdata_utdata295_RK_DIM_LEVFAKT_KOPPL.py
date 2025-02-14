
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'KORR1': 'varchar(max)',
 'KORR10': 'varchar(max)',
 'KORR2': 'varchar(max)',
 'KORR3': 'varchar(max)',
 'KORR4': 'varchar(max)',
 'KORR5': 'varchar(max)',
 'KORR6': 'varchar(max)',
 'KORR7': 'varchar(max)',
 'KORR8': 'varchar(max)',
 'KORR9': 'varchar(max)',
 'NR': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(KORR1 AS VARCHAR(MAX)) AS KORR1,
CAST(KORR10 AS VARCHAR(MAX)) AS KORR10,
CAST(KORR2 AS VARCHAR(MAX)) AS KORR2,
CAST(KORR3 AS VARCHAR(MAX)) AS KORR3,
CAST(KORR4 AS VARCHAR(MAX)) AS KORR4,
CAST(KORR5 AS VARCHAR(MAX)) AS KORR5,
CAST(KORR6 AS VARCHAR(MAX)) AS KORR6,
CAST(KORR7 AS VARCHAR(MAX)) AS KORR7,
CAST(KORR8 AS VARCHAR(MAX)) AS KORR8,
CAST(KORR9 AS VARCHAR(MAX)) AS KORR9,
CAST(NR AS VARCHAR(MAX)) AS NR FROM utdata.utdata295.RK_DIM_LEVFAKT_KOPPL"""
    return pipe(query=query)
