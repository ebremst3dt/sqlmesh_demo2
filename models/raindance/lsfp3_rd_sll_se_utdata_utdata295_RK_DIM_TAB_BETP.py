
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_BETP': 'varchar(max)',
 'TAB_BETP_ID_TEXT': 'varchar(max)',
 'TAB_BETP_TEXT': 'varchar(max)',
 'VARDE1': 'varchar(max)'},
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
		CAST(TAB_BETP AS VARCHAR(MAX)) AS tab_betp,
		CAST(TAB_BETP_ID_TEXT AS VARCHAR(MAX)) AS tab_betp_id_text,
		CAST(TAB_BETP_TEXT AS VARCHAR(MAX)) AS tab_betp_text,
		CAST(VARDE1 AS VARCHAR(MAX)) AS varde1 
	FROM utdata.utdata295.RK_DIM_TAB_BETP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
