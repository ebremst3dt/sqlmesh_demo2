
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'PROC_DATUM_FOM': 'datetime',
 'PROC_DATUM_TOM': 'datetime',
 'PROC_GILTIG_FOM': 'datetime',
 'PROC_GILTIG_TOM': 'datetime',
 'PROC_ID': 'varchar(1)',
 'PROC_ID_TEXT': 'varchar(32)',
 'PROC_PASSIV': 'bit',
 'PROC_TEXT': 'varchar(30)'},
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
 		CAST(PROC_DATUM_FOM AS VARCHAR(MAX)) AS proc_datum_fom,
		CAST(PROC_DATUM_TOM AS VARCHAR(MAX)) AS proc_datum_tom,
		CAST(PROC_GILTIG_FOM AS VARCHAR(MAX)) AS proc_giltig_fom,
		CAST(PROC_GILTIG_TOM AS VARCHAR(MAX)) AS proc_giltig_tom,
		CAST(PROC_ID AS VARCHAR(MAX)) AS proc_id,
		CAST(PROC_ID_TEXT AS VARCHAR(MAX)) AS proc_id_text,
		CAST(PROC_PASSIV AS VARCHAR(MAX)) AS proc_passiv,
		CAST(PROC_TEXT AS VARCHAR(MAX)) AS proc_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_PROC
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
