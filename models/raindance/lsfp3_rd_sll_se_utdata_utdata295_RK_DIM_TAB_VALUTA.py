
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_VALUTA': 'varchar(max)',
 'TAB_VALUTA_ID_TEXT': 'varchar(max)',
 'TAB_VALUTA_TEXT': 'varchar(max)',
 'VARDE2': 'varchar(max)'},
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
CAST(TAB_VALUTA AS VARCHAR(MAX)) AS TAB_VALUTA,
CAST(TAB_VALUTA_ID_TEXT AS VARCHAR(MAX)) AS TAB_VALUTA_ID_TEXT,
CAST(TAB_VALUTA_TEXT AS VARCHAR(MAX)) AS TAB_VALUTA_TEXT,
CAST(VARDE2 AS VARCHAR(MAX)) AS VARDE2 FROM utdata.utdata295.RK_DIM_TAB_VALUTA"""
    return pipe(query=query)
