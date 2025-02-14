
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_RDEB': 'varchar(max)',
 'TAB_RDEB_ID_TEXT': 'varchar(max)',
 'TAB_RDEB_TEXT': 'varchar(max)',
 'VARDE1': 'varchar(max)',
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
    query = """
	SELECT top 1000
 		CAST(DUMMY2 AS VARCHAR(MAX)) AS dummy2,
		CAST(TAB_RDEB AS VARCHAR(MAX)) AS tab_rdeb,
		CAST(TAB_RDEB_ID_TEXT AS VARCHAR(MAX)) AS tab_rdeb_id_text,
		CAST(TAB_RDEB_TEXT AS VARCHAR(MAX)) AS tab_rdeb_text,
		CAST(VARDE1 AS VARCHAR(MAX)) AS varde1,
		CAST(VARDE2 AS VARCHAR(MAX)) AS varde2 
	FROM utdata.utdata295.RK_DIM_TAB_RDEB
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
