
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'VALUTA_GILTIG_FOM': 'varchar(max)',
 'VALUTA_GILTIG_TOM': 'varchar(max)',
 'VALUTA_ID': 'varchar(max)',
 'VALUTA_ID_TEXT': 'varchar(max)',
 'VALUTA_PASSIV': 'varchar(max)',
 'VALUTA_TEXT': 'varchar(max)'},
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
 		CAST(VALUTA_GILTIG_FOM AS VARCHAR(MAX)) AS valuta_giltig_fom,
		CAST(VALUTA_GILTIG_TOM AS VARCHAR(MAX)) AS valuta_giltig_tom,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CAST(VALUTA_ID_TEXT AS VARCHAR(MAX)) AS valuta_id_text,
		CAST(VALUTA_PASSIV AS VARCHAR(MAX)) AS valuta_passiv,
		CAST(VALUTA_TEXT AS VARCHAR(MAX)) AS valuta_text 
	FROM utdata.utdata295.EK_DIM_OBJ_VALUTA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
