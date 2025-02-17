
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BORAD_DATUM_FOM': 'datetime',
 'BORAD_DATUM_TOM': 'datetime',
 'BORAD_GILTIG_FOM': 'datetime',
 'BORAD_GILTIG_TOM': 'datetime',
 'BORAD_ID': 'varchar(3)',
 'BORAD_ID_TEXT': 'varchar(34)',
 'BORAD_PASSIV': 'bit',
 'BORAD_TEXT': 'varchar(30)'},
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
 		CAST(BORAD_DATUM_FOM AS VARCHAR(MAX)) AS borad_datum_fom,
		CAST(BORAD_DATUM_TOM AS VARCHAR(MAX)) AS borad_datum_tom,
		CAST(BORAD_GILTIG_FOM AS VARCHAR(MAX)) AS borad_giltig_fom,
		CAST(BORAD_GILTIG_TOM AS VARCHAR(MAX)) AS borad_giltig_tom,
		CAST(BORAD_ID AS VARCHAR(MAX)) AS borad_id,
		CAST(BORAD_ID_TEXT AS VARCHAR(MAX)) AS borad_id_text,
		CAST(BORAD_PASSIV AS VARCHAR(MAX)) AS borad_passiv,
		CAST(BORAD_TEXT AS VARCHAR(MAX)) AS borad_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_BORAD
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
