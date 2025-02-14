
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'FAKTNR': 'varchar(max)', 'ORDERNR': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(FAKTNR AS VARCHAR(MAX)) AS FAKTNR,
CAST(ORDERNR AS VARCHAR(MAX)) AS ORDERNR FROM utdata.utdata295.RK_ORDERKOPPL"""
    return pipe(query=query)
