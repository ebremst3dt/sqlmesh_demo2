
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'VALUTA_DATUM_FOM': 'datetime',
 'VALUTA_DATUM_TOM': 'datetime',
 'VALUTA_GILTIG_FOM': 'datetime',
 'VALUTA_GILTIG_TOM': 'datetime',
 'VALUTA_ID': 'varchar(3)',
 'VALUTA_ID_TEXT': 'varchar(34)',
 'VALUTA_PASSIV': 'bit',
 'VALUTA_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), VALUTA_DATUM_FOM, 126) AS valuta_datum_fom,
		CONVERT(varchar(max), VALUTA_DATUM_TOM, 126) AS valuta_datum_tom,
		CONVERT(varchar(max), VALUTA_GILTIG_FOM, 126) AS valuta_giltig_fom,
		CONVERT(varchar(max), VALUTA_GILTIG_TOM, 126) AS valuta_giltig_tom,
		CAST(VALUTA_ID AS VARCHAR(MAX)) AS valuta_id,
		CAST(VALUTA_ID_TEXT AS VARCHAR(MAX)) AS valuta_id_text,
		CAST(VALUTA_PASSIV AS VARCHAR(MAX)) AS valuta_passiv,
		CAST(VALUTA_TEXT AS VARCHAR(MAX)) AS valuta_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_VALUTA
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
