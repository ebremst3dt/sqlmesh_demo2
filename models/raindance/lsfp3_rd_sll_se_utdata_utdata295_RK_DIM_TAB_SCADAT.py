
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_SCADAT': 'varchar(max)',
 'TAB_SCADAT_ID_TEXT': 'varchar(max)',
 'TAB_SCADAT_TEXT': 'varchar(max)'},
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
	SELECT TOP 1000 top 1000
 		CAST(DUMMY2 AS VARCHAR(MAX)) AS dummy2,
		CAST(TAB_SCADAT AS VARCHAR(MAX)) AS tab_scadat,
		CAST(TAB_SCADAT_ID_TEXT AS VARCHAR(MAX)) AS tab_scadat_id_text,
		CAST(TAB_SCADAT_TEXT AS VARCHAR(MAX)) AS tab_scadat_text 
	FROM utdata.utdata295.RK_DIM_TAB_SCADAT
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
