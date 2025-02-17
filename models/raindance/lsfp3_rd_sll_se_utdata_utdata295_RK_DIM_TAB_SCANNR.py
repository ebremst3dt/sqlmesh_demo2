
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_SCANNR': 'varchar(max)',
 'TAB_SCANNR_ID_TEXT': 'varchar(max)',
 'TAB_SCANNR_TEXT': 'varchar(max)'},
    kind=kind.FullKind,
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """
	SELECT top 1000
 		CAST(DUMMY2 AS VARCHAR(MAX)) AS dummy2,
		CAST(TAB_SCANNR AS VARCHAR(MAX)) AS tab_scannr,
		CAST(TAB_SCANNR_ID_TEXT AS VARCHAR(MAX)) AS tab_scannr_id_text,
		CAST(TAB_SCANNR_TEXT AS VARCHAR(MAX)) AS tab_scannr_text 
	FROM utdata.utdata295.RK_DIM_TAB_SCANNR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
