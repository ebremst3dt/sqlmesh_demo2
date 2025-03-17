
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KGR_GILTIG_FOM': 'varchar(max)', 'KGR_GILTIG_TOM': 'varchar(max)', 'KGR_ID': 'varchar(max)', 'KGR_ID_TEXT': 'varchar(max)', 'KGR_PASSIV': 'varchar(max)', 'KGR_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)'},
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
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS kkl_text 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_KGR ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    