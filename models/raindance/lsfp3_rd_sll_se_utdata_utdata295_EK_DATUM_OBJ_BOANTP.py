
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BOANTP_DATUM_FOM': 'varchar(max)',
 'BOANTP_DATUM_TOM': 'varchar(max)',
 'BOANTP_GILTIG_FOM': 'varchar(max)',
 'BOANTP_GILTIG_TOM': 'varchar(max)',
 'BOANTP_ID': 'varchar(max)',
 'BOANTP_ID_TEXT': 'varchar(max)',
 'BOANTP_PASSIV': 'varchar(max)',
 'BOANTP_TEXT': 'varchar(max)'},
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
	SELECT TOP 1000 top 1000
 		CONVERT(varchar(max), BOANTP_DATUM_FOM, 126) AS boantp_datum_fom,
		CONVERT(varchar(max), BOANTP_DATUM_TOM, 126) AS boantp_datum_tom,
		CONVERT(varchar(max), BOANTP_GILTIG_FOM, 126) AS boantp_giltig_fom,
		CONVERT(varchar(max), BOANTP_GILTIG_TOM, 126) AS boantp_giltig_tom,
		CAST(BOANTP_ID AS VARCHAR(MAX)) AS boantp_id,
		CAST(BOANTP_ID_TEXT AS VARCHAR(MAX)) AS boantp_id_text,
		CAST(BOANTP_PASSIV AS VARCHAR(MAX)) AS boantp_passiv,
		CAST(BOANTP_TEXT AS VARCHAR(MAX)) AS boantp_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_BOANTP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
