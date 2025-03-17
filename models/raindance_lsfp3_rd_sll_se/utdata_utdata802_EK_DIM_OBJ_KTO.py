
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'COGNCO_GILTIG_FOM': 'varchar(max)', 'COGNCO_GILTIG_TOM': 'varchar(max)', 'COGNCO_ID': 'varchar(max)', 'COGNCO_ID_TEXT': 'varchar(max)', 'COGNCO_PASSIV': 'varchar(max)', 'COGNCO_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata802' as _source,
		CONVERT(varchar(max), COGNCO_GILTIG_FOM, 126) AS cognco_giltig_fom,
		CONVERT(varchar(max), COGNCO_GILTIG_TOM, 126) AS cognco_giltig_tom,
		CAST(COGNCO_ID AS VARCHAR(MAX)) AS cognco_id,
		CAST(COGNCO_ID_TEXT AS VARCHAR(MAX)) AS cognco_id_text,
		CAST(COGNCO_PASSIV AS VARCHAR(MAX)) AS cognco_passiv,
		CAST(COGNCO_TEXT AS VARCHAR(MAX)) AS cognco_text,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS kgrupp_giltig_fom,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS kgrupp_giltig_tom,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS kgrupp_id,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS kgrupp_id_text,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS kgrupp_passiv,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS kgrupp_text,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS kkl_giltig_fom,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS kkl_giltig_tom,
		CAST(KKL_ID AS VARCHAR(MAX)) AS kkl_id,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS kkl_id_text,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS kkl_passiv,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS kkl_text,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS kto_giltig_fom,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS kto_giltig_tom,
		CAST(KTO_ID AS VARCHAR(MAX)) AS kto_id,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS kto_id_text,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS kto_passiv,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS kto_text,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS tsik_giltig_fom,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS tsik_giltig_tom,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS tsik_id,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS tsik_id_text,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS tsik_passiv,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS tsik_text 
	FROM utdata.utdata802.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    