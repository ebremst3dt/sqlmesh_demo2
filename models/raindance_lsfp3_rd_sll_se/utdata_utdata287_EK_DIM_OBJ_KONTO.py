
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'KGRR_GILTIG_FOM': 'varchar(max)', 'KGRR_GILTIG_TOM': 'varchar(max)', 'KGRR_ID': 'varchar(max)', 'KGRR_ID_TEXT': 'varchar(max)', 'KGRR_PASSIV': 'varchar(max)', 'KGRR_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKLASS_GILTIG_FOM': 'varchar(max)', 'KKLASS_GILTIG_TOM': 'varchar(max)', 'KKLASS_ID': 'varchar(max)', 'KKLASS_ID_TEXT': 'varchar(max)', 'KKLASS_PASSIV': 'varchar(max)', 'KKLASS_TEXT': 'varchar(max)', 'KKLRR_GILTIG_FOM': 'varchar(max)', 'KKLRR_GILTIG_TOM': 'varchar(max)', 'KKLRR_ID': 'varchar(max)', 'KKLRR_ID_TEXT': 'varchar(max)', 'KKLRR_PASSIV': 'varchar(max)', 'KKLRR_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'TSIK_GILTIG_FOM': 'varchar(max)', 'TSIK_GILTIG_TOM': 'varchar(max)', 'TSIK_ID': 'varchar(max)', 'TSIK_ID_TEXT': 'varchar(max)', 'TSIK_PASSIV': 'varchar(max)', 'TSIK_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata287' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS frango_giltig_fom,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS frango_giltig_tom,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS frango_id,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS frango_id_text,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS frango_passiv,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS frango_text,
		CONVERT(varchar(max), KGRR_GILTIG_FOM, 126) AS kgrr_giltig_fom,
		CONVERT(varchar(max), KGRR_GILTIG_TOM, 126) AS kgrr_giltig_tom,
		CAST(KGRR_ID AS VARCHAR(MAX)) AS kgrr_id,
		CAST(KGRR_ID_TEXT AS VARCHAR(MAX)) AS kgrr_id_text,
		CAST(KGRR_PASSIV AS VARCHAR(MAX)) AS kgrr_passiv,
		CAST(KGRR_TEXT AS VARCHAR(MAX)) AS kgrr_text,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS kgrupp_giltig_fom,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS kgrupp_giltig_tom,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS kgrupp_id,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS kgrupp_id_text,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS kgrupp_passiv,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS kgrupp_text,
		CONVERT(varchar(max), KKLASS_GILTIG_FOM, 126) AS kklass_giltig_fom,
		CONVERT(varchar(max), KKLASS_GILTIG_TOM, 126) AS kklass_giltig_tom,
		CAST(KKLASS_ID AS VARCHAR(MAX)) AS kklass_id,
		CAST(KKLASS_ID_TEXT AS VARCHAR(MAX)) AS kklass_id_text,
		CAST(KKLASS_PASSIV AS VARCHAR(MAX)) AS kklass_passiv,
		CAST(KKLASS_TEXT AS VARCHAR(MAX)) AS kklass_text,
		CONVERT(varchar(max), KKLRR_GILTIG_FOM, 126) AS kklrr_giltig_fom,
		CONVERT(varchar(max), KKLRR_GILTIG_TOM, 126) AS kklrr_giltig_tom,
		CAST(KKLRR_ID AS VARCHAR(MAX)) AS kklrr_id,
		CAST(KKLRR_ID_TEXT AS VARCHAR(MAX)) AS kklrr_id_text,
		CAST(KKLRR_PASSIV AS VARCHAR(MAX)) AS kklrr_passiv,
		CAST(KKLRR_TEXT AS VARCHAR(MAX)) AS kklrr_text,
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
		CAST(TSIK_TEXT AS VARCHAR(MAX)) AS tsik_text 
	FROM utdata.utdata287.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    