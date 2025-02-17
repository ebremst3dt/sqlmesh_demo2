
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'FAKTSTATUS': 'varchar(max)',
 'FAKTSTATUSTYP': 'varchar(max)',
 'FAKTSTATUSTYP_TEXT': 'varchar(max)',
 'FAKTSTATUS_TEXT': 'varchar(max)'},
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
 		CAST(FAKTSTATUS AS VARCHAR(MAX)) AS faktstatus,
		CAST(FAKTSTATUS_TEXT AS VARCHAR(MAX)) AS faktstatus_text,
		CAST(FAKTSTATUSTYP AS VARCHAR(MAX)) AS faktstatustyp,
		CAST(FAKTSTATUSTYP_TEXT AS VARCHAR(MAX)) AS faktstatustyp_text 
	FROM utdata.utdata295.RK_DIM_STATUS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
