
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FR01_GILTIG_FOM': 'varchar(max)', 'FR01_GILTIG_TOM': 'varchar(max)', 'FR01_ID': 'varchar(max)', 'FR01_ID_TEXT': 'varchar(max)', 'FR01_PASSIV': 'varchar(max)', 'FR01_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KTOG_GILTIG_FOM': 'varchar(max)', 'KTOG_GILTIG_TOM': 'varchar(max)', 'KTOG_ID': 'varchar(max)', 'KTOG_ID_TEXT': 'varchar(max)', 'KTOG_PASSIV': 'varchar(max)', 'KTOG_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)'},
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
		'mcp1_rd_sll_se_MediCarrierUDP_utdata100' as _source,
		CONVERT(varchar(max), FR01_GILTIG_FOM, 126) AS fr01_giltig_fom,
		CONVERT(varchar(max), FR01_GILTIG_TOM, 126) AS fr01_giltig_tom,
		CAST(FR01_ID AS VARCHAR(MAX)) AS fr01_id,
		CAST(FR01_ID_TEXT AS VARCHAR(MAX)) AS fr01_id_text,
		CAST(FR01_PASSIV AS VARCHAR(MAX)) AS fr01_passiv,
		CAST(FR01_TEXT AS VARCHAR(MAX)) AS fr01_text,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS kkl_giltig_fom,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS kkl_giltig_tom,
		CAST(KKL_ID AS VARCHAR(MAX)) AS kkl_id,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS kkl_id_text,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS kkl_passiv,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS kkl_text,
		CONVERT(varchar(max), KTOG_GILTIG_FOM, 126) AS ktog_giltig_fom,
		CONVERT(varchar(max), KTOG_GILTIG_TOM, 126) AS ktog_giltig_tom,
		CAST(KTOG_ID AS VARCHAR(MAX)) AS ktog_id,
		CAST(KTOG_ID_TEXT AS VARCHAR(MAX)) AS ktog_id_text,
		CAST(KTOG_PASSIV AS VARCHAR(MAX)) AS ktog_passiv,
		CAST(KTOG_TEXT AS VARCHAR(MAX)) AS ktog_text,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS kto_giltig_fom,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS kto_giltig_tom,
		CAST(KTO_ID AS VARCHAR(MAX)) AS kto_id,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS kto_id_text,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS kto_passiv,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS kto_text 
	FROM MediCarrierUDP.utdata100.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="mcp1.rd.sll.se")
    