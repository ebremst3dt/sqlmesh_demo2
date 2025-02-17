
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(2)',
 'TAB_BETV': 'varchar(2)',
 'TAB_BETV_ID_TEXT': 'varchar(120)',
 'TAB_BETV_TEXT': 'varchar(80)',
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
		CAST(TAB_BETV AS VARCHAR(MAX)) AS tab_betv,
		CAST(TAB_BETV_ID_TEXT AS VARCHAR(MAX)) AS tab_betv_id_text,
		CAST(TAB_BETV_TEXT AS VARCHAR(MAX)) AS tab_betv_text,
		CAST(VARDE1 AS VARCHAR(MAX)) AS varde1,
		CAST(VARDE2 AS VARCHAR(MAX)) AS varde2 
	FROM utdata.utdata295.RK_DIM_TAB_BETV
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
