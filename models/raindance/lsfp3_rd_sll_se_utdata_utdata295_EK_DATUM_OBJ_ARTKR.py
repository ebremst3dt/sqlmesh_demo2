
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'ARTKR_DATUM_FOM': 'varchar(max)',
 'ARTKR_DATUM_TOM': 'varchar(max)',
 'ARTKR_GILTIG_FOM': 'varchar(max)',
 'ARTKR_GILTIG_TOM': 'varchar(max)',
 'ARTKR_ID': 'varchar(max)',
 'ARTKR_ID_TEXT': 'varchar(max)',
 'ARTKR_PASSIV': 'varchar(max)',
 'ARTKR_TEXT': 'varchar(max)'},
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
 		CAST(ARTKR_DATUM_FOM AS VARCHAR(MAX)) AS artkr_datum_fom,
		CAST(ARTKR_DATUM_TOM AS VARCHAR(MAX)) AS artkr_datum_tom,
		CAST(ARTKR_GILTIG_FOM AS VARCHAR(MAX)) AS artkr_giltig_fom,
		CAST(ARTKR_GILTIG_TOM AS VARCHAR(MAX)) AS artkr_giltig_tom,
		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(ARTKR_ID_TEXT AS VARCHAR(MAX)) AS artkr_id_text,
		CAST(ARTKR_PASSIV AS VARCHAR(MAX)) AS artkr_passiv,
		CAST(ARTKR_TEXT AS VARCHAR(MAX)) AS artkr_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_ARTKR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
