
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ATTRIBUTE': 'varchar(max)',
 'ATTR_KEY_PAT': 'varchar(max)',
 'SBID': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ATTR_KEY_PAT AS VARCHAR(MAX)) AS ATTR_KEY_PAT,
CAST(ATTRIBUTE AS VARCHAR(MAX)) AS ATTRIBUTE,
CAST(SBID AS VARCHAR(MAX)) AS SBID FROM utdata.utdata295.RK_DIM_LEV_ATTR"""
    return pipe(query=query)
