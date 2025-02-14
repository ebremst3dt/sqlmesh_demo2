
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'ARTKR_ID': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'UTFALL_V': 'varchar(max)'},
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
 		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v 
	FROM utdata.utdata295.EK_FAKTA_VARDE_ARTKR$UTFALL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
