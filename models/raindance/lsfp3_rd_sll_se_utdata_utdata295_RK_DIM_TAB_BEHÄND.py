
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_BEHÄND': 'varchar(max)',
 'TAB_BEHÄND_ID_TEXT': 'varchar(max)',
 'TAB_BEHÄND_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(DUMMY2 AS VARCHAR(MAX)) AS DUMMY2,
CAST(TAB_BEHÄND AS VARCHAR(MAX)) AS TAB_BEHÄND,
CAST(TAB_BEHÄND_ID_TEXT AS VARCHAR(MAX)) AS TAB_BEHÄND_ID_TEXT,
CAST(TAB_BEHÄND_TEXT AS VARCHAR(MAX)) AS TAB_BEHÄND_TEXT FROM utdata.utdata295.RK_DIM_TAB_BEHÄND"""
    return pipe(query=query)
