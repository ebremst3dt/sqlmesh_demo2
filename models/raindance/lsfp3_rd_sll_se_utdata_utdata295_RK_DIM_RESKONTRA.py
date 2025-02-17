
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'EXTERN': 'varchar(max)',
 'EXTERN_TEXT': 'varchar(max)',
 'RESKONTRA': 'varchar(max)',
 'RESKONTRA_TEXT': 'varchar(max)',
 'RESKTYP': 'varchar(max)',
 'RESKTYP_TEXT': 'varchar(max)'},
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
 		CAST(EXTERN AS VARCHAR(MAX)) AS extern,
		CAST(EXTERN_TEXT AS VARCHAR(MAX)) AS extern_text,
		CAST(RESKONTRA AS VARCHAR(MAX)) AS reskontra,
		CAST(RESKONTRA_TEXT AS VARCHAR(MAX)) AS reskontra_text,
		CAST(RESKTYP AS VARCHAR(MAX)) AS resktyp,
		CAST(RESKTYP_TEXT AS VARCHAR(MAX)) AS resktyp_text 
	FROM utdata.utdata295.RK_DIM_RESKONTRA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
