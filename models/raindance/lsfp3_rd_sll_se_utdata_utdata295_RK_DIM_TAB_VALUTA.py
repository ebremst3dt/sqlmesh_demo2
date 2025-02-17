
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_VALUTA': 'varchar(max)',
 'TAB_VALUTA_ID_TEXT': 'varchar(max)',
 'TAB_VALUTA_TEXT': 'varchar(max)',
 'VARDE2': 'varchar(max)'},
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
		CAST(TAB_VALUTA AS VARCHAR(MAX)) AS tab_valuta,
		CAST(TAB_VALUTA_ID_TEXT AS VARCHAR(MAX)) AS tab_valuta_id_text,
		CAST(TAB_VALUTA_TEXT AS VARCHAR(MAX)) AS tab_valuta_text,
		CAST(VARDE2 AS VARCHAR(MAX)) AS varde2 
	FROM utdata.utdata295.RK_DIM_TAB_VALUTA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
