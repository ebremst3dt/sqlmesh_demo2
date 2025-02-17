
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'ERSGR_GILTIG_FOM': 'varchar(max)',
 'ERSGR_GILTIG_TOM': 'varchar(max)',
 'ERSGR_ID': 'varchar(max)',
 'ERSGR_ID_TEXT': 'varchar(max)',
 'ERSGR_PASSIV': 'varchar(max)',
 'ERSGR_TEXT': 'varchar(max)'},
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
 		CAST(ERSGR_GILTIG_FOM AS VARCHAR(MAX)) AS ersgr_giltig_fom,
		CAST(ERSGR_GILTIG_TOM AS VARCHAR(MAX)) AS ersgr_giltig_tom,
		CAST(ERSGR_ID AS VARCHAR(MAX)) AS ersgr_id,
		CAST(ERSGR_ID_TEXT AS VARCHAR(MAX)) AS ersgr_id_text,
		CAST(ERSGR_PASSIV AS VARCHAR(MAX)) AS ersgr_passiv,
		CAST(ERSGR_TEXT AS VARCHAR(MAX)) AS ersgr_text 
	FROM utdata.utdata295.EK_DIM_OBJ_ERSGR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
