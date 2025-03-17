
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'HKRAD_GILTIG_FOM': 'varchar(max)', 'HKRAD_GILTIG_TOM': 'varchar(max)', 'HKRAD_ID': 'varchar(max)', 'HKRAD_ID_TEXT': 'varchar(max)', 'HKRAD_PASSIV': 'varchar(max)', 'HKRAD_TEXT': 'varchar(max)', 'KGR_GILTIG_FOM': 'varchar(max)', 'KGR_GILTIG_TOM': 'varchar(max)', 'KGR_ID': 'varchar(max)', 'KGR_ID_TEXT': 'varchar(max)', 'KGR_PASSIV': 'varchar(max)', 'KGR_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KLIRAD_GILTIG_FOM': 'varchar(max)', 'KLIRAD_GILTIG_TOM': 'varchar(max)', 'KLIRAD_ID': 'varchar(max)', 'KLIRAD_ID_TEXT': 'varchar(max)', 'KLIRAD_PASSIV': 'varchar(max)', 'KLIRAD_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'SRU_GILTIG_FOM': 'varchar(max)', 'SRU_GILTIG_TOM': 'varchar(max)', 'SRU_ID': 'varchar(max)', 'SRU_ID_TEXT': 'varchar(max)', 'SRU_PASSIV': 'varchar(max)', 'SRU_TEXT': 'varchar(max)', 'STYRAD_GILTIG_FOM': 'varchar(max)', 'STYRAD_GILTIG_TOM': 'varchar(max)', 'STYRAD_ID': 'varchar(max)', 'STYRAD_ID_TEXT': 'varchar(max)', 'STYRAD_PASSIV': 'varchar(max)', 'STYRAD_TEXT': 'varchar(max)'},
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
		'ftvddbs08_ftv_sll_se_ftvudp_ftv_400' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS frango_giltig_fom,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS frango_giltig_tom,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS frango_id,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS frango_id_text,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS frango_passiv,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS frango_text,
		CONVERT(varchar(max), HKRAD_GILTIG_FOM, 126) AS hkrad_giltig_fom,
		CONVERT(varchar(max), HKRAD_GILTIG_TOM, 126) AS hkrad_giltig_tom,
		CAST(HKRAD_ID AS VARCHAR(MAX)) AS hkrad_id,
		CAST(HKRAD_ID_TEXT AS VARCHAR(MAX)) AS hkrad_id_text,
		CAST(HKRAD_PASSIV AS VARCHAR(MAX)) AS hkrad_passiv,
		CAST(HKRAD_TEXT AS VARCHAR(MAX)) AS hkrad_text,
		CONVERT(varchar(max), KGR_GILTIG_FOM, 126) AS kgr_giltig_fom,
		CONVERT(varchar(max), KGR_GILTIG_TOM, 126) AS kgr_giltig_tom,
		CAST(KGR_ID AS VARCHAR(MAX)) AS kgr_id,
		CAST(KGR_ID_TEXT AS VARCHAR(MAX)) AS kgr_id_text,
		CAST(KGR_PASSIV AS VARCHAR(MAX)) AS kgr_passiv,
		CAST(KGR_TEXT AS VARCHAR(MAX)) AS kgr_text,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS kkl_giltig_fom,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS kkl_giltig_tom,
		CAST(KKL_ID AS VARCHAR(MAX)) AS kkl_id,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS kkl_id_text,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS kkl_passiv,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS kkl_text,
		CONVERT(varchar(max), KLIRAD_GILTIG_FOM, 126) AS klirad_giltig_fom,
		CONVERT(varchar(max), KLIRAD_GILTIG_TOM, 126) AS klirad_giltig_tom,
		CAST(KLIRAD_ID AS VARCHAR(MAX)) AS klirad_id,
		CAST(KLIRAD_ID_TEXT AS VARCHAR(MAX)) AS klirad_id_text,
		CAST(KLIRAD_PASSIV AS VARCHAR(MAX)) AS klirad_passiv,
		CAST(KLIRAD_TEXT AS VARCHAR(MAX)) AS klirad_text,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS konto_giltig_fom,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS konto_giltig_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS konto_id_text,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS konto_passiv,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS konto_text,
		CONVERT(varchar(max), SRU_GILTIG_FOM, 126) AS sru_giltig_fom,
		CONVERT(varchar(max), SRU_GILTIG_TOM, 126) AS sru_giltig_tom,
		CAST(SRU_ID AS VARCHAR(MAX)) AS sru_id,
		CAST(SRU_ID_TEXT AS VARCHAR(MAX)) AS sru_id_text,
		CAST(SRU_PASSIV AS VARCHAR(MAX)) AS sru_passiv,
		CAST(SRU_TEXT AS VARCHAR(MAX)) AS sru_text,
		CONVERT(varchar(max), STYRAD_GILTIG_FOM, 126) AS styrad_giltig_fom,
		CONVERT(varchar(max), STYRAD_GILTIG_TOM, 126) AS styrad_giltig_tom,
		CAST(STYRAD_ID AS VARCHAR(MAX)) AS styrad_id,
		CAST(STYRAD_ID_TEXT AS VARCHAR(MAX)) AS styrad_id_text,
		CAST(STYRAD_PASSIV AS VARCHAR(MAX)) AS styrad_passiv,
		CAST(STYRAD_TEXT AS VARCHAR(MAX)) AS styrad_text 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    