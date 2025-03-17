
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
		CONVERT(varchar(max), FRG2_GILTIG_FOM, 126) AS frg2_giltig_fom,
		CONVERT(varchar(max), FRG2_GILTIG_TOM, 126) AS frg2_giltig_tom,
		CAST(FRG2_ID AS VARCHAR(MAX)) AS frg2_id,
		CAST(FRG2_ID_TEXT AS VARCHAR(MAX)) AS frg2_id_text,
		CAST(FRG2_PASSIV AS VARCHAR(MAX)) AS frg2_passiv,
		CAST(FRG2_TEXT AS VARCHAR(MAX)) AS frg2_text,
		CONVERT(varchar(max), FRG_GILTIG_FOM, 126) AS frg_giltig_fom,
		CONVERT(varchar(max), FRG_GILTIG_TOM, 126) AS frg_giltig_tom,
		CAST(FRG_ID AS VARCHAR(MAX)) AS frg_id,
		CAST(FRG_ID_TEXT AS VARCHAR(MAX)) AS frg_id_text,
		CAST(FRG_PASSIV AS VARCHAR(MAX)) AS frg_passiv,
		CAST(FRG_TEXT AS VARCHAR(MAX)) AS frg_text,
		CONVERT(varchar(max), FRK_GILTIG_FOM, 126) AS frk_giltig_fom,
		CONVERT(varchar(max), FRK_GILTIG_TOM, 126) AS frk_giltig_tom,
		CAST(FRK_ID AS VARCHAR(MAX)) AS frk_id,
		CAST(FRK_ID_TEXT AS VARCHAR(MAX)) AS frk_id_text,
		CAST(FRK_PASSIV AS VARCHAR(MAX)) AS frk_passiv,
		CAST(FRK_TEXT AS VARCHAR(MAX)) AS frk_text,
		CONVERT(varchar(max), K1_GILTIG_FOM, 126) AS k1_giltig_fom,
		CONVERT(varchar(max), K1_GILTIG_TOM, 126) AS k1_giltig_tom,
		CAST(K1_ID AS VARCHAR(MAX)) AS k1_id,
		CAST(K1_ID_TEXT AS VARCHAR(MAX)) AS k1_id_text,
		CAST(K1_PASSIV AS VARCHAR(MAX)) AS k1_passiv,
		CAST(K1_TEXT AS VARCHAR(MAX)) AS k1_text,
		CONVERT(varchar(max), K2_GILTIG_FOM, 126) AS k2_giltig_fom,
		CONVERT(varchar(max), K2_GILTIG_TOM, 126) AS k2_giltig_tom,
		CAST(K2_ID AS VARCHAR(MAX)) AS k2_id,
		CAST(K2_ID_TEXT AS VARCHAR(MAX)) AS k2_id_text,
		CAST(K2_PASSIV AS VARCHAR(MAX)) AS k2_passiv,
		CAST(K2_TEXT AS VARCHAR(MAX)) AS k2_text,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS konto_giltig_fom,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS konto_giltig_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS konto_id_text,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS konto_passiv,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS konto_text,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS tsik_giltig_fom,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS tsik_giltig_tom,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS tsik_id,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS tsik_id_text,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS tsik_passiv,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS tsik_text,
		CONVERT(varchar(max), UHK_GILTIG_FOM, 126) AS uhk_giltig_fom,
		CONVERT(varchar(max), UHK_GILTIG_TOM, 126) AS uhk_giltig_tom,
		CAST(UHK_ID AS VARCHAR(MAX)) AS uhk_id,
		CAST(UHK_ID_TEXT AS VARCHAR(MAX)) AS uhk_id_text,
		CAST(UHK_PASSIV AS VARCHAR(MAX)) AS uhk_passiv,
		CAST(UHK_TEXT AS VARCHAR(MAX)) AS uhk_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    