
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'NIVA': 'varchar(max)', 'NUMERISK_VANSTER': 'varchar(max)', 'RADNUMMER': 'varchar(max)', 'RADTILLHOR': 'varchar(max)', 'STYRANDE_ID': 'varchar(max)', 'STYRANDE_INTERVALL': 'varchar(max)', 'STYRANDE_INTERVALL2': 'varchar(max)', 'STYRANDE_NR': 'varchar(max)', 'STYRANDE_OBJEKT_FOM': 'varchar(max)', 'STYRANDE_OBJEKT_TOM': 'varchar(max)', 'STYRANDE_STJARNURV': 'varchar(max)', 'STYRD_ID': 'varchar(max)', 'STYRD_INTERVALL': 'varchar(max)', 'STYRD_INTERVALL2': 'varchar(max)', 'STYRD_NR': 'varchar(max)', 'STYRD_STJARNURV': 'varchar(max)', 'VILLKAR': 'varchar(max)', 'VILLKAR_NR': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.FULL
    ),
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
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
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
	FROM utdata.utdata295.EK_SAMBAND_STYRANDE) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    