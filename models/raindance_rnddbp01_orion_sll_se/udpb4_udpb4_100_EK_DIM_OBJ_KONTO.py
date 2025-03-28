
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRG2_GILTIG_FOM': 'varchar(max)', 'FRG2_GILTIG_TOM': 'varchar(max)', 'FRG2_ID': 'varchar(max)', 'FRG2_ID_TEXT': 'varchar(max)', 'FRG2_PASSIV': 'varchar(max)', 'FRG2_TEXT': 'varchar(max)', 'FRG_GILTIG_FOM': 'varchar(max)', 'FRG_GILTIG_TOM': 'varchar(max)', 'FRG_ID': 'varchar(max)', 'FRG_ID_TEXT': 'varchar(max)', 'FRG_PASSIV': 'varchar(max)', 'FRG_TEXT': 'varchar(max)', 'FRK_GILTIG_FOM': 'varchar(max)', 'FRK_GILTIG_TOM': 'varchar(max)', 'FRK_ID': 'varchar(max)', 'FRK_ID_TEXT': 'varchar(max)', 'FRK_PASSIV': 'varchar(max)', 'FRK_TEXT': 'varchar(max)', 'K1_GILTIG_FOM': 'varchar(max)', 'K1_GILTIG_TOM': 'varchar(max)', 'K1_ID': 'varchar(max)', 'K1_ID_TEXT': 'varchar(max)', 'K1_PASSIV': 'varchar(max)', 'K1_TEXT': 'varchar(max)', 'K2_GILTIG_FOM': 'varchar(max)', 'K2_GILTIG_TOM': 'varchar(max)', 'K2_ID': 'varchar(max)', 'K2_ID_TEXT': 'varchar(max)', 'K2_PASSIV': 'varchar(max)', 'K2_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)', 'UHK_GILTIG_FOM': 'varchar(max)', 'UHK_GILTIG_TOM': 'varchar(max)', 'UHK_ID': 'varchar(max)', 'UHK_ID_TEXT': 'varchar(max)', 'UHK_PASSIV': 'varchar(max)', 'UHK_TEXT': 'varchar(max)'},
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
    query = f"""
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CONVERT(varchar(max), FRG2_GILTIG_FOM, 126) AS FRG2_GILTIG_FOM,
		CONVERT(varchar(max), FRG2_GILTIG_TOM, 126) AS FRG2_GILTIG_TOM,
		CAST(FRG2_ID AS VARCHAR(MAX)) AS FRG2_ID,
		CAST(FRG2_ID_TEXT AS VARCHAR(MAX)) AS FRG2_ID_TEXT,
		CAST(FRG2_PASSIV AS VARCHAR(MAX)) AS FRG2_PASSIV,
		CAST(FRG2_TEXT AS VARCHAR(MAX)) AS FRG2_TEXT,
		CONVERT(varchar(max), FRG_GILTIG_FOM, 126) AS FRG_GILTIG_FOM,
		CONVERT(varchar(max), FRG_GILTIG_TOM, 126) AS FRG_GILTIG_TOM,
		CAST(FRG_ID AS VARCHAR(MAX)) AS FRG_ID,
		CAST(FRG_ID_TEXT AS VARCHAR(MAX)) AS FRG_ID_TEXT,
		CAST(FRG_PASSIV AS VARCHAR(MAX)) AS FRG_PASSIV,
		CAST(FRG_TEXT AS VARCHAR(MAX)) AS FRG_TEXT,
		CONVERT(varchar(max), FRK_GILTIG_FOM, 126) AS FRK_GILTIG_FOM,
		CONVERT(varchar(max), FRK_GILTIG_TOM, 126) AS FRK_GILTIG_TOM,
		CAST(FRK_ID AS VARCHAR(MAX)) AS FRK_ID,
		CAST(FRK_ID_TEXT AS VARCHAR(MAX)) AS FRK_ID_TEXT,
		CAST(FRK_PASSIV AS VARCHAR(MAX)) AS FRK_PASSIV,
		CAST(FRK_TEXT AS VARCHAR(MAX)) AS FRK_TEXT,
		CONVERT(varchar(max), K1_GILTIG_FOM, 126) AS K1_GILTIG_FOM,
		CONVERT(varchar(max), K1_GILTIG_TOM, 126) AS K1_GILTIG_TOM,
		CAST(K1_ID AS VARCHAR(MAX)) AS K1_ID,
		CAST(K1_ID_TEXT AS VARCHAR(MAX)) AS K1_ID_TEXT,
		CAST(K1_PASSIV AS VARCHAR(MAX)) AS K1_PASSIV,
		CAST(K1_TEXT AS VARCHAR(MAX)) AS K1_TEXT,
		CONVERT(varchar(max), K2_GILTIG_FOM, 126) AS K2_GILTIG_FOM,
		CONVERT(varchar(max), K2_GILTIG_TOM, 126) AS K2_GILTIG_TOM,
		CAST(K2_ID AS VARCHAR(MAX)) AS K2_ID,
		CAST(K2_ID_TEXT AS VARCHAR(MAX)) AS K2_ID_TEXT,
		CAST(K2_PASSIV AS VARCHAR(MAX)) AS K2_PASSIV,
		CAST(K2_TEXT AS VARCHAR(MAX)) AS K2_TEXT,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS KONTO_GILTIG_FOM,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS KONTO_GILTIG_TOM,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS KONTO_ID_TEXT,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS KONTO_PASSIV,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS KONTO_TEXT,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS TSIK_GILTIG_FOM,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS TSIK_GILTIG_TOM,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS TSIK_ID,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS TSIK_ID_TEXT,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS TSIK_PASSIV,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS TSIK_TEXT,
		CONVERT(varchar(max), UHK_GILTIG_FOM, 126) AS UHK_GILTIG_FOM,
		CONVERT(varchar(max), UHK_GILTIG_TOM, 126) AS UHK_GILTIG_TOM,
		CAST(UHK_ID AS VARCHAR(MAX)) AS UHK_ID,
		CAST(UHK_ID_TEXT AS VARCHAR(MAX)) AS UHK_ID_TEXT,
		CAST(UHK_PASSIV AS VARCHAR(MAX)) AS UHK_PASSIV,
		CAST(UHK_TEXT AS VARCHAR(MAX)) AS UHK_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    