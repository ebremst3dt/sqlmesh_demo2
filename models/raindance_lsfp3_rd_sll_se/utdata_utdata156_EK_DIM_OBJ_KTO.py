
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKGR_GILTIG_FOM': 'varchar(max)', 'KKGR_GILTIG_TOM': 'varchar(max)', 'KKGR_ID': 'varchar(max)', 'KKGR_ID_TEXT': 'varchar(max)', 'KKGR_PASSIV': 'varchar(max)', 'KKGR_TEXT': 'varchar(max)', 'KKL_GILTIG_FOM': 'varchar(max)', 'KKL_GILTIG_TOM': 'varchar(max)', 'KKL_ID': 'varchar(max)', 'KKL_ID_TEXT': 'varchar(max)', 'KKL_PASSIV': 'varchar(max)', 'KKL_TEXT': 'varchar(max)', 'KK_GILTIG_FOM': 'varchar(max)', 'KK_GILTIG_TOM': 'varchar(max)', 'KK_ID': 'varchar(max)', 'KK_ID_TEXT': 'varchar(max)', 'KK_PASSIV': 'varchar(max)', 'KK_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)', 'R3_GILTIG_FOM': 'varchar(max)', 'R3_GILTIG_TOM': 'varchar(max)', 'R3_ID': 'varchar(max)', 'R3_ID_TEXT': 'varchar(max)', 'R3_PASSIV': 'varchar(max)', 'R3_TEXT': 'varchar(max)', 'R4_GILTIG_FOM': 'varchar(max)', 'R4_GILTIG_TOM': 'varchar(max)', 'R4_ID': 'varchar(max)', 'R4_ID_TEXT': 'varchar(max)', 'R4_PASSIV': 'varchar(max)', 'R4_TEXT': 'varchar(max)', 'RRBR_GILTIG_FOM': 'varchar(max)', 'RRBR_GILTIG_TOM': 'varchar(max)', 'RRBR_ID': 'varchar(max)', 'RRBR_ID_TEXT': 'varchar(max)', 'RRBR_PASSIV': 'varchar(max)', 'RRBR_TEXT': 'varchar(max)', 'STÖKGR_GILTIG_FOM': 'varchar(max)', 'STÖKGR_GILTIG_TOM': 'varchar(max)', 'STÖKGR_ID': 'varchar(max)', 'STÖKGR_ID_TEXT': 'varchar(max)', 'STÖKGR_PASSIV': 'varchar(max)', 'STÖKGR_TEXT': 'varchar(max)', 'STÖKOD_GILTIG_FOM': 'varchar(max)', 'STÖKOD_GILTIG_TOM': 'varchar(max)', 'STÖKOD_ID': 'varchar(max)', 'STÖKOD_ID_TEXT': 'varchar(max)', 'STÖKOD_PASSIV': 'varchar(max)', 'STÖKOD_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata156' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS frango_giltig_fom,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS frango_giltig_tom,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS frango_id,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS frango_id_text,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS frango_passiv,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS frango_text,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS kgrupp_giltig_fom,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS kgrupp_giltig_tom,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS kgrupp_id,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS kgrupp_id_text,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS kgrupp_passiv,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS kgrupp_text,
		CONVERT(varchar(max), KKGR_GILTIG_FOM, 126) AS kkgr_giltig_fom,
		CONVERT(varchar(max), KKGR_GILTIG_TOM, 126) AS kkgr_giltig_tom,
		CAST(KKGR_ID AS VARCHAR(MAX)) AS kkgr_id,
		CAST(KKGR_ID_TEXT AS VARCHAR(MAX)) AS kkgr_id_text,
		CAST(KKGR_PASSIV AS VARCHAR(MAX)) AS kkgr_passiv,
		CAST(KKGR_TEXT AS VARCHAR(MAX)) AS kkgr_text,
		CONVERT(varchar(max), KKL_GILTIG_FOM, 126) AS kkl_giltig_fom,
		CONVERT(varchar(max), KKL_GILTIG_TOM, 126) AS kkl_giltig_tom,
		CAST(KKL_ID AS VARCHAR(MAX)) AS kkl_id,
		CAST(KKL_ID_TEXT AS VARCHAR(MAX)) AS kkl_id_text,
		CAST(KKL_PASSIV AS VARCHAR(MAX)) AS kkl_passiv,
		CAST(KKL_TEXT AS VARCHAR(MAX)) AS kkl_text,
		CONVERT(varchar(max), KK_GILTIG_FOM, 126) AS kk_giltig_fom,
		CONVERT(varchar(max), KK_GILTIG_TOM, 126) AS kk_giltig_tom,
		CAST(KK_ID AS VARCHAR(MAX)) AS kk_id,
		CAST(KK_ID_TEXT AS VARCHAR(MAX)) AS kk_id_text,
		CAST(KK_PASSIV AS VARCHAR(MAX)) AS kk_passiv,
		CAST(KK_TEXT AS VARCHAR(MAX)) AS kk_text,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS kto_giltig_fom,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS kto_giltig_tom,
		CAST(KTO_ID AS VARCHAR(MAX)) AS kto_id,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS kto_id_text,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS kto_passiv,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS kto_text,
		CONVERT(varchar(max), R3_GILTIG_FOM, 126) AS r3_giltig_fom,
		CONVERT(varchar(max), R3_GILTIG_TOM, 126) AS r3_giltig_tom,
		CAST(R3_ID AS VARCHAR(MAX)) AS r3_id,
		CAST(R3_ID_TEXT AS VARCHAR(MAX)) AS r3_id_text,
		CAST(R3_PASSIV AS VARCHAR(MAX)) AS r3_passiv,
		CAST(R3_TEXT AS VARCHAR(MAX)) AS r3_text,
		CONVERT(varchar(max), R4_GILTIG_FOM, 126) AS r4_giltig_fom,
		CONVERT(varchar(max), R4_GILTIG_TOM, 126) AS r4_giltig_tom,
		CAST(R4_ID AS VARCHAR(MAX)) AS r4_id,
		CAST(R4_ID_TEXT AS VARCHAR(MAX)) AS r4_id_text,
		CAST(R4_PASSIV AS VARCHAR(MAX)) AS r4_passiv,
		CAST(R4_TEXT AS VARCHAR(MAX)) AS r4_text,
		CONVERT(varchar(max), RRBR_GILTIG_FOM, 126) AS rrbr_giltig_fom,
		CONVERT(varchar(max), RRBR_GILTIG_TOM, 126) AS rrbr_giltig_tom,
		CAST(RRBR_ID AS VARCHAR(MAX)) AS rrbr_id,
		CAST(RRBR_ID_TEXT AS VARCHAR(MAX)) AS rrbr_id_text,
		CAST(RRBR_PASSIV AS VARCHAR(MAX)) AS rrbr_passiv,
		CAST(RRBR_TEXT AS VARCHAR(MAX)) AS rrbr_text,
		CONVERT(varchar(max), STÖKGR_GILTIG_FOM, 126) AS stökgr_giltig_fom,
		CONVERT(varchar(max), STÖKGR_GILTIG_TOM, 126) AS stökgr_giltig_tom,
		CAST(STÖKGR_ID AS VARCHAR(MAX)) AS stökgr_id,
		CAST(STÖKGR_ID_TEXT AS VARCHAR(MAX)) AS stökgr_id_text,
		CAST(STÖKGR_PASSIV AS VARCHAR(MAX)) AS stökgr_passiv,
		CAST(STÖKGR_TEXT AS VARCHAR(MAX)) AS stökgr_text,
		CONVERT(varchar(max), STÖKOD_GILTIG_FOM, 126) AS stökod_giltig_fom,
		CONVERT(varchar(max), STÖKOD_GILTIG_TOM, 126) AS stökod_giltig_tom,
		CAST(STÖKOD_ID AS VARCHAR(MAX)) AS stökod_id,
		CAST(STÖKOD_ID_TEXT AS VARCHAR(MAX)) AS stökod_id_text,
		CAST(STÖKOD_PASSIV AS VARCHAR(MAX)) AS stökod_passiv,
		CAST(STÖKOD_TEXT AS VARCHAR(MAX)) AS stökod_text,
		CONVERT(varchar(max), TSIK_GILTIG_FOM, 126) AS tsik_giltig_fom,
		CONVERT(varchar(max), TSIK_GILTIG_TOM, 126) AS tsik_giltig_tom,
		CAST(TSIK_ID AS VARCHAR(MAX)) AS tsik_id,
		CAST(TSIK_ID_TEXT AS VARCHAR(MAX)) AS tsik_id_text,
		CAST(TSIK_PASSIV AS VARCHAR(MAX)) AS tsik_passiv,
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS tsik_text 
	FROM utdata.utdata156.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    