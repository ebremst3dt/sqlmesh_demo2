
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KONSUL_GILTIG_FOM': 'varchar(max)', 'KONSUL_GILTIG_TOM': 'varchar(max)', 'KONSUL_ID': 'varchar(max)', 'KONSUL_ID_TEXT': 'varchar(max)', 'KONSUL_PASSIV': 'varchar(max)', 'KONSUL_TEXT': 'varchar(max)', 'TANV_GILTIG_FOM': 'varchar(max)', 'TANV_GILTIG_TOM': 'varchar(max)', 'TANV_ID': 'varchar(max)', 'TANV_ID_TEXT': 'varchar(max)', 'TANV_PASSIV': 'varchar(max)', 'TANV_TEXT': 'varchar(max)', 'TKONS_GILTIG_FOM': 'varchar(max)', 'TKONS_GILTIG_TOM': 'varchar(max)', 'TKONS_ID': 'varchar(max)', 'TKONS_ID_TEXT': 'varchar(max)', 'TKONS_PASSIV': 'varchar(max)', 'TKONS_TEXT': 'varchar(max)', 'TRESKL_GILTIG_FOM': 'varchar(max)', 'TRESKL_GILTIG_TOM': 'varchar(max)', 'TRESKL_ID': 'varchar(max)', 'TRESKL_ID_TEXT': 'varchar(max)', 'TRESKL_PASSIV': 'varchar(max)', 'TRESKL_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), KONSUL_GILTIG_FOM, 126) AS konsul_giltig_fom,
		CONVERT(varchar(max), KONSUL_GILTIG_TOM, 126) AS konsul_giltig_tom,
		CAST(KONSUL_ID AS VARCHAR(MAX)) AS konsul_id,
		CAST(KONSUL_ID_TEXT AS VARCHAR(MAX)) AS konsul_id_text,
		CAST(KONSUL_PASSIV AS VARCHAR(MAX)) AS konsul_passiv,
		CAST(KONSUL_TEXT AS VARCHAR(MAX)) AS konsul_text,
		CONVERT(varchar(max), TANV_GILTIG_FOM, 126) AS tanv_giltig_fom,
		CONVERT(varchar(max), TANV_GILTIG_TOM, 126) AS tanv_giltig_tom,
		CAST(TANV_ID AS VARCHAR(MAX)) AS tanv_id,
		CAST(TANV_ID_TEXT AS VARCHAR(MAX)) AS tanv_id_text,
		CAST(TANV_PASSIV AS VARCHAR(MAX)) AS tanv_passiv,
		CAST(TANV_TEXT AS VARCHAR(MAX)) AS tanv_text,
		CONVERT(varchar(max), TKONS_GILTIG_FOM, 126) AS tkons_giltig_fom,
		CONVERT(varchar(max), TKONS_GILTIG_TOM, 126) AS tkons_giltig_tom,
		CAST(TKONS_ID AS VARCHAR(MAX)) AS tkons_id,
		CAST(TKONS_ID_TEXT AS VARCHAR(MAX)) AS tkons_id_text,
		CAST(TKONS_PASSIV AS VARCHAR(MAX)) AS tkons_passiv,
		CAST(TKONS_TEXT AS VARCHAR(MAX)) AS tkons_text,
		CONVERT(varchar(max), TRESKL_GILTIG_FOM, 126) AS treskl_giltig_fom,
		CONVERT(varchar(max), TRESKL_GILTIG_TOM, 126) AS treskl_giltig_tom,
		CAST(TRESKL_ID AS VARCHAR(MAX)) AS treskl_id,
		CAST(TRESKL_ID_TEXT AS VARCHAR(MAX)) AS treskl_id_text,
		CAST(TRESKL_PASSIV AS VARCHAR(MAX)) AS treskl_passiv,
		CAST(TRESKL_TEXT AS VARCHAR(MAX)) AS treskl_text 
	FROM utdata.utdata298.EK_DIM_OBJ_TANV ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    