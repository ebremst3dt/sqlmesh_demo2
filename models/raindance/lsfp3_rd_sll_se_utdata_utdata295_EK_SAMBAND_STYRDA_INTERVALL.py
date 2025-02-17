
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'INTRADNUMMER': 'numeric',
 'RADNUMMER': 'numeric',
 'STYRD_ID': 'varchar(6)',
 'STYRD_NR': 'numeric',
 'STYRT_INTERVALL': 'varchar(41)',
 'STYRT_INTERVALL2': 'varchar(41)',
 'STYRT_OBJEKT_FOM': 'varchar(20)',
 'STYRT_OBJEKT_TOM': 'varchar(20)'},
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
 		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
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
