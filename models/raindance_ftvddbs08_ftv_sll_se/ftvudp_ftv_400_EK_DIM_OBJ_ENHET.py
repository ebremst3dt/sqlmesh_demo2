
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSVAR_GILTIG_FOM': 'varchar(max)', 'ANSVAR_GILTIG_TOM': 'varchar(max)', 'ANSVAR_ID': 'varchar(max)', 'ANSVAR_ID_TEXT': 'varchar(max)', 'ANSVAR_PASSIV': 'varchar(max)', 'ANSVAR_TEXT': 'varchar(max)', 'ENHET_GILTIG_FOM': 'varchar(max)', 'ENHET_GILTIG_TOM': 'varchar(max)', 'ENHET_ID': 'varchar(max)', 'ENHET_ID_TEXT': 'varchar(max)', 'ENHET_PASSIV': 'varchar(max)', 'ENHET_TEXT': 'varchar(max)', 'TVC_GILTIG_FOM': 'varchar(max)', 'TVC_GILTIG_TOM': 'varchar(max)', 'TVC_ID': 'varchar(max)', 'TVC_ID_TEXT': 'varchar(max)', 'TVC_PASSIV': 'varchar(max)', 'TVC_TEXT': 'varchar(max)', 'VERKRS_GILTIG_FOM': 'varchar(max)', 'VERKRS_GILTIG_TOM': 'varchar(max)', 'VERKRS_ID': 'varchar(max)', 'VERKRS_ID_TEXT': 'varchar(max)', 'VERKRS_PASSIV': 'varchar(max)', 'VERKRS_TEXT': 'varchar(max)', 'VERKS_GILTIG_FOM': 'varchar(max)', 'VERKS_GILTIG_TOM': 'varchar(max)', 'VERKS_ID': 'varchar(max)', 'VERKS_ID_TEXT': 'varchar(max)', 'VERKS_PASSIV': 'varchar(max)', 'VERKS_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ANSVAR_GILTIG_FOM, 126) AS ansvar_giltig_fom,
		CONVERT(varchar(max), ANSVAR_GILTIG_TOM, 126) AS ansvar_giltig_tom,
		CAST(ANSVAR_ID AS VARCHAR(MAX)) AS ansvar_id,
		CAST(ANSVAR_ID_TEXT AS VARCHAR(MAX)) AS ansvar_id_text,
		CAST(ANSVAR_PASSIV AS VARCHAR(MAX)) AS ansvar_passiv,
		CAST(ANSVAR_TEXT AS VARCHAR(MAX)) AS ansvar_text,
		CONVERT(varchar(max), ENHET_GILTIG_FOM, 126) AS enhet_giltig_fom,
		CONVERT(varchar(max), ENHET_GILTIG_TOM, 126) AS enhet_giltig_tom,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS enhet_id,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS enhet_id_text,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS enhet_passiv,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS enhet_text,
		CONVERT(varchar(max), TVC_GILTIG_FOM, 126) AS tvc_giltig_fom,
		CONVERT(varchar(max), TVC_GILTIG_TOM, 126) AS tvc_giltig_tom,
		CAST(TVC_ID AS VARCHAR(MAX)) AS tvc_id,
		CAST(TVC_ID_TEXT AS VARCHAR(MAX)) AS tvc_id_text,
		CAST(TVC_PASSIV AS VARCHAR(MAX)) AS tvc_passiv,
		CAST(TVC_TEXT AS VARCHAR(MAX)) AS tvc_text,
		CONVERT(varchar(max), VERKRS_GILTIG_FOM, 126) AS verkrs_giltig_fom,
		CONVERT(varchar(max), VERKRS_GILTIG_TOM, 126) AS verkrs_giltig_tom,
		CAST(VERKRS_ID AS VARCHAR(MAX)) AS verkrs_id,
		CAST(VERKRS_ID_TEXT AS VARCHAR(MAX)) AS verkrs_id_text,
		CAST(VERKRS_PASSIV AS VARCHAR(MAX)) AS verkrs_passiv,
		CAST(VERKRS_TEXT AS VARCHAR(MAX)) AS verkrs_text,
		CONVERT(varchar(max), VERKS_GILTIG_FOM, 126) AS verks_giltig_fom,
		CONVERT(varchar(max), VERKS_GILTIG_TOM, 126) AS verks_giltig_tom,
		CAST(VERKS_ID AS VARCHAR(MAX)) AS verks_id,
		CAST(VERKS_ID_TEXT AS VARCHAR(MAX)) AS verks_id_text,
		CAST(VERKS_PASSIV AS VARCHAR(MAX)) AS verks_passiv,
		CAST(VERKS_TEXT AS VARCHAR(MAX)) AS verks_text,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS vgren_giltig_fom,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_ENHET ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    