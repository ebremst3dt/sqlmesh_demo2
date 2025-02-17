
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
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
 		CAST(BOANTP_DATUM_FOM AS VARCHAR(MAX)) AS boantp_datum_fom,
		CAST(BOANTP_DATUM_TOM AS VARCHAR(MAX)) AS boantp_datum_tom,
		CAST(BOANTP_GILTIG_FOM AS VARCHAR(MAX)) AS boantp_giltig_fom,
		CAST(BOANTP_GILTIG_TOM AS VARCHAR(MAX)) AS boantp_giltig_tom,
		CAST(BOANTP_ID AS VARCHAR(MAX)) AS boantp_id,
		CAST(BOANTP_ID_TEXT AS VARCHAR(MAX)) AS boantp_id_text,
		CAST(BOANTP_PASSIV AS VARCHAR(MAX)) AS boantp_passiv,
		CAST(BOANTP_TEXT AS VARCHAR(MAX)) AS boantp_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_BOANTP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
