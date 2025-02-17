
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ERSGR_DATUM_FOM': 'datetime',
 'ERSGR_DATUM_TOM': 'datetime',
 'ERSGR_GILTIG_FOM': 'datetime',
 'ERSGR_GILTIG_TOM': 'datetime',
 'ERSGR_ID': 'varchar(3)',
 'ERSGR_ID_TEXT': 'varchar(34)',
 'ERSGR_PASSIV': 'bit',
 'ERSGR_TEXT': 'varchar(30)'},
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
 		CAST(ERSGR_DATUM_FOM AS VARCHAR(MAX)) AS ersgr_datum_fom,
		CAST(ERSGR_DATUM_TOM AS VARCHAR(MAX)) AS ersgr_datum_tom,
		CAST(ERSGR_GILTIG_FOM AS VARCHAR(MAX)) AS ersgr_giltig_fom,
		CAST(ERSGR_GILTIG_TOM AS VARCHAR(MAX)) AS ersgr_giltig_tom,
		CAST(ERSGR_ID AS VARCHAR(MAX)) AS ersgr_id,
		CAST(ERSGR_ID_TEXT AS VARCHAR(MAX)) AS ersgr_id_text,
		CAST(ERSGR_PASSIV AS VARCHAR(MAX)) AS ersgr_passiv,
		CAST(ERSGR_TEXT AS VARCHAR(MAX)) AS ersgr_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_ERSGR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
