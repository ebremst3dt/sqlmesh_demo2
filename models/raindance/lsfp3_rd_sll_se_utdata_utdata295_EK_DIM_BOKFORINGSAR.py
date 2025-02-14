
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BOKFORINGSAR': 'varchar(max)',
 'BOKFORINGSARSLUT': 'varchar(max)',
 'BOKFORINGSAR_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BOKFORINGSAR AS VARCHAR(MAX)) AS BOKFORINGSAR,
CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS BOKFORINGSAR_TEXT,
CAST(BOKFORINGSARSLUT AS VARCHAR(MAX)) AS BOKFORINGSARSLUT FROM utdata.utdata295.EK_DIM_BOKFORINGSAR"""
    return pipe(query=query)
