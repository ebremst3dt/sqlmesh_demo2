
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(4)',
 'TAB_AVTTYP': 'varchar(4)',
 'TAB_AVTTYP_ID_TEXT': 'varchar(120)',
 'TAB_AVTTYP_TEXT': 'varchar(80)'},
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
		CAST(TAB_AVTTYP AS VARCHAR(MAX)) AS tab_avttyp,
		CAST(TAB_AVTTYP_ID_TEXT AS VARCHAR(MAX)) AS tab_avttyp_id_text,
		CAST(TAB_AVTTYP_TEXT AS VARCHAR(MAX)) AS tab_avttyp_text 
	FROM utdata.utdata295.RK_DIM_TAB_AVTTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
