
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'INTRADNUMMER': 'varchar(max)',
 'RADNUMMER': 'varchar(max)',
 'STYRD_ID': 'varchar(max)',
 'STYRD_NR': 'varchar(max)',
 'STYRT_INTERVALL': 'varchar(max)',
 'STYRT_INTERVALL2': 'varchar(max)',
 'STYRT_OBJEKT_FOM': 'varchar(max)',
 'STYRT_OBJEKT_TOM': 'varchar(max)'},
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
 		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(INTRADNUMMER AS VARCHAR(MAX)) AS intradnummer,
		CAST(RADNUMMER AS VARCHAR(MAX)) AS radnummer,
		CAST(STYRD_ID AS VARCHAR(MAX)) AS styrd_id,
		CAST(STYRD_NR AS VARCHAR(MAX)) AS styrd_nr,
		CAST(STYRT_INTERVALL AS VARCHAR(MAX)) AS styrt_intervall,
		CAST(STYRT_INTERVALL2 AS VARCHAR(MAX)) AS styrt_intervall2,
		CAST(STYRT_OBJEKT_FOM AS VARCHAR(MAX)) AS styrt_objekt_fom,
		CAST(STYRT_OBJEKT_TOM AS VARCHAR(MAX)) AS styrt_objekt_tom 
	FROM utdata.utdata295.EK_SAMBAND_STYRDA_INTERVALL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
