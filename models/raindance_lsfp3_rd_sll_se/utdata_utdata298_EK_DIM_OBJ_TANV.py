
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
		CONVERT(varchar(max), KONSUL_GILTIG_FOM, 126) AS KONSUL_GILTIG_FOM,
		CONVERT(varchar(max), KONSUL_GILTIG_TOM, 126) AS KONSUL_GILTIG_TOM,
		CAST(KONSUL_ID AS VARCHAR(MAX)) AS KONSUL_ID,
		CAST(KONSUL_ID_TEXT AS VARCHAR(MAX)) AS KONSUL_ID_TEXT,
		CAST(KONSUL_PASSIV AS VARCHAR(MAX)) AS KONSUL_PASSIV,
		CAST(KONSUL_TEXT AS VARCHAR(MAX)) AS KONSUL_TEXT,
		CONVERT(varchar(max), TANV_GILTIG_FOM, 126) AS TANV_GILTIG_FOM,
		CONVERT(varchar(max), TANV_GILTIG_TOM, 126) AS TANV_GILTIG_TOM,
		CAST(TANV_ID AS VARCHAR(MAX)) AS TANV_ID,
		CAST(TANV_ID_TEXT AS VARCHAR(MAX)) AS TANV_ID_TEXT,
		CAST(TANV_PASSIV AS VARCHAR(MAX)) AS TANV_PASSIV,
		CAST(TANV_TEXT AS VARCHAR(MAX)) AS TANV_TEXT,
		CONVERT(varchar(max), TKONS_GILTIG_FOM, 126) AS TKONS_GILTIG_FOM,
		CONVERT(varchar(max), TKONS_GILTIG_TOM, 126) AS TKONS_GILTIG_TOM,
		CAST(TKONS_ID AS VARCHAR(MAX)) AS TKONS_ID,
		CAST(TKONS_ID_TEXT AS VARCHAR(MAX)) AS TKONS_ID_TEXT,
		CAST(TKONS_PASSIV AS VARCHAR(MAX)) AS TKONS_PASSIV,
		CAST(TKONS_TEXT AS VARCHAR(MAX)) AS TKONS_TEXT,
		CONVERT(varchar(max), TRESKL_GILTIG_FOM, 126) AS TRESKL_GILTIG_FOM,
		CONVERT(varchar(max), TRESKL_GILTIG_TOM, 126) AS TRESKL_GILTIG_TOM,
		CAST(TRESKL_ID AS VARCHAR(MAX)) AS TRESKL_ID,
		CAST(TRESKL_ID_TEXT AS VARCHAR(MAX)) AS TRESKL_ID_TEXT,
		CAST(TRESKL_PASSIV AS VARCHAR(MAX)) AS TRESKL_PASSIV,
		CAST(TRESKL_TEXT AS VARCHAR(MAX)) AS TRESKL_TEXT 
	FROM utdata.utdata298.EK_DIM_OBJ_TANV ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    