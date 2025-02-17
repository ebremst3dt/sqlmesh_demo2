
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'FÖPROC_DATUM_FOM': 'varchar(max)',
 'FÖPROC_DATUM_TOM': 'varchar(max)',
 'FÖPROC_GILTIG_FOM': 'varchar(max)',
 'FÖPROC_GILTIG_TOM': 'varchar(max)',
 'FÖPROC_ID': 'varchar(max)',
 'FÖPROC_ID_TEXT': 'varchar(max)',
 'FÖPROC_PASSIV': 'varchar(max)',
 'FÖPROC_TEXT': 'varchar(max)'},
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
 		CAST(FÖPROC_DATUM_FOM AS VARCHAR(MAX)) AS föproc_datum_fom,
		CAST(FÖPROC_DATUM_TOM AS VARCHAR(MAX)) AS föproc_datum_tom,
		CAST(FÖPROC_GILTIG_FOM AS VARCHAR(MAX)) AS föproc_giltig_fom,
		CAST(FÖPROC_GILTIG_TOM AS VARCHAR(MAX)) AS föproc_giltig_tom,
		CAST(FÖPROC_ID AS VARCHAR(MAX)) AS föproc_id,
		CAST(FÖPROC_ID_TEXT AS VARCHAR(MAX)) AS föproc_id_text,
		CAST(FÖPROC_PASSIV AS VARCHAR(MAX)) AS föproc_passiv,
		CAST(FÖPROC_TEXT AS VARCHAR(MAX)) AS föproc_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_FÖPROC
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
