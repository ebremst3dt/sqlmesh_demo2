
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'KRAVNIVA': 'varchar(max)', 'KRAVNIVA_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(KRAVNIVA AS VARCHAR(MAX)) AS KRAVNIVA,
CAST(KRAVNIVA_TEXT AS VARCHAR(MAX)) AS KRAVNIVA_TEXT FROM utdata.utdata295.RK_DIM_KRAVNIVA"""
    return pipe(query=query)
