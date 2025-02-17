
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ARTKR_DATUM_FOM': 'datetime',
 'ARTKR_DATUM_TOM': 'datetime',
 'ARTKR_GILTIG_FOM': 'datetime',
 'ARTKR_GILTIG_TOM': 'datetime',
 'ARTKR_ID': 'varchar(10)',
 'ARTKR_ID_TEXT': 'varchar(41)',
 'ARTKR_PASSIV': 'bit',
 'ARTKR_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), ARTKR_DATUM_FOM, 126) AS artkr_datum_fom,
		CONVERT(varchar(max), ARTKR_DATUM_TOM, 126) AS artkr_datum_tom,
		CONVERT(varchar(max), ARTKR_GILTIG_FOM, 126) AS artkr_giltig_fom,
		CONVERT(varchar(max), ARTKR_GILTIG_TOM, 126) AS artkr_giltig_tom,
		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(ARTKR_ID_TEXT AS VARCHAR(MAX)) AS artkr_id_text,
		CAST(ARTKR_PASSIV AS VARCHAR(MAX)) AS artkr_passiv,
		CAST(ARTKR_TEXT AS VARCHAR(MAX)) AS artkr_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_ARTKR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
