
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(2)',
 'TAB_UBK': 'varchar(2)',
 'TAB_UBK_ID_TEXT': 'varchar(120)',
 'TAB_UBK_TEXT': 'varchar(80)',
 'VARDE1': 'numeric',
 'VARDE2': 'numeric'},
    kind=ModelKindName.FULL,
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
		CAST(TAB_UBK AS VARCHAR(MAX)) AS tab_ubk,
		CAST(TAB_UBK_ID_TEXT AS VARCHAR(MAX)) AS tab_ubk_id_text,
		CAST(TAB_UBK_TEXT AS VARCHAR(MAX)) AS tab_ubk_text,
		CAST(VARDE1 AS VARCHAR(MAX)) AS varde1,
		CAST(VARDE2 AS VARCHAR(MAX)) AS varde2 
	FROM utdata.utdata295.RK_DIM_TAB_UBK
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
