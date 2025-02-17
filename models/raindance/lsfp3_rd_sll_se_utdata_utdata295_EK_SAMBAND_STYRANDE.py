
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'NIVA': 'numeric',
 'NUMERISK_VANSTER': 'varchar(1)',
 'RADNUMMER': 'numeric',
 'RADTILLHOR': 'numeric',
 'STYRANDE_ID': 'varchar(6)',
 'STYRANDE_INTERVALL': 'varchar(41)',
 'STYRANDE_INTERVALL2': 'varchar(41)',
 'STYRANDE_NR': 'numeric',
 'STYRANDE_OBJEKT_FOM': 'varchar(20)',
 'STYRANDE_OBJEKT_TOM': 'varchar(20)',
 'STYRANDE_STJARNURV': 'varchar(1)',
 'STYRD_ID': 'varchar(6)',
 'STYRD_INTERVALL': 'varchar(4000)',
 'STYRD_INTERVALL2': 'varchar(4000)',
 'STYRD_NR': 'numeric',
 'STYRD_STJARNURV': 'varchar(1)',
 'VILLKAR': 'varchar(1)',
 'VILLKAR_NR': 'numeric'},
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
		CAST(NIVA AS VARCHAR(MAX)) AS niva,
		CAST(NUMERISK_VANSTER AS VARCHAR(MAX)) AS numerisk_vanster,
		CAST(RADNUMMER AS VARCHAR(MAX)) AS radnummer,
		CAST(RADTILLHOR AS VARCHAR(MAX)) AS radtillhor,
		CAST(STYRANDE_ID AS VARCHAR(MAX)) AS styrande_id,
		CAST(STYRANDE_INTERVALL AS VARCHAR(MAX)) AS styrande_intervall,
		CAST(STYRANDE_INTERVALL2 AS VARCHAR(MAX)) AS styrande_intervall2,
		CAST(STYRANDE_NR AS VARCHAR(MAX)) AS styrande_nr,
		CAST(STYRANDE_OBJEKT_FOM AS VARCHAR(MAX)) AS styrande_objekt_fom,
		CAST(STYRANDE_OBJEKT_TOM AS VARCHAR(MAX)) AS styrande_objekt_tom,
		CAST(STYRANDE_STJARNURV AS VARCHAR(MAX)) AS styrande_stjarnurv,
		CAST(STYRD_ID AS VARCHAR(MAX)) AS styrd_id,
		CAST(STYRD_INTERVALL AS VARCHAR(MAX)) AS styrd_intervall,
		CAST(STYRD_INTERVALL2 AS VARCHAR(MAX)) AS styrd_intervall2,
		CAST(STYRD_NR AS VARCHAR(MAX)) AS styrd_nr,
		CAST(STYRD_STJARNURV AS VARCHAR(MAX)) AS styrd_stjarnurv,
		CAST(VILLKAR AS VARCHAR(MAX)) AS villkar,
		CAST(VILLKAR_NR AS VARCHAR(MAX)) AS villkar_nr 
	FROM utdata.utdata295.EK_SAMBAND_STYRANDE
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
