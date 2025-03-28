
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
		CONVERT(varchar(max), ANSVAR_GILTIG_FOM, 126) AS ANSVAR_GILTIG_FOM,
		CONVERT(varchar(max), ANSVAR_GILTIG_TOM, 126) AS ANSVAR_GILTIG_TOM,
		CAST(ANSVAR_ID AS VARCHAR(MAX)) AS ANSVAR_ID,
		CAST(ANSVAR_ID_TEXT AS VARCHAR(MAX)) AS ANSVAR_ID_TEXT,
		CAST(ANSVAR_PASSIV AS VARCHAR(MAX)) AS ANSVAR_PASSIV,
		CAST(ANSVAR_TEXT AS VARCHAR(MAX)) AS ANSVAR_TEXT,
		CONVERT(varchar(max), ENHET_GILTIG_FOM, 126) AS ENHET_GILTIG_FOM,
		CONVERT(varchar(max), ENHET_GILTIG_TOM, 126) AS ENHET_GILTIG_TOM,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS ENHET_ID,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS ENHET_ID_TEXT,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS ENHET_PASSIV,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS ENHET_TEXT,
		CONVERT(varchar(max), TVC_GILTIG_FOM, 126) AS TVC_GILTIG_FOM,
		CONVERT(varchar(max), TVC_GILTIG_TOM, 126) AS TVC_GILTIG_TOM,
		CAST(TVC_ID AS VARCHAR(MAX)) AS TVC_ID,
		CAST(TVC_ID_TEXT AS VARCHAR(MAX)) AS TVC_ID_TEXT,
		CAST(TVC_PASSIV AS VARCHAR(MAX)) AS TVC_PASSIV,
		CAST(TVC_TEXT AS VARCHAR(MAX)) AS TVC_TEXT,
		CONVERT(varchar(max), VERKRS_GILTIG_FOM, 126) AS VERKRS_GILTIG_FOM,
		CONVERT(varchar(max), VERKRS_GILTIG_TOM, 126) AS VERKRS_GILTIG_TOM,
		CAST(VERKRS_ID AS VARCHAR(MAX)) AS VERKRS_ID,
		CAST(VERKRS_ID_TEXT AS VARCHAR(MAX)) AS VERKRS_ID_TEXT,
		CAST(VERKRS_PASSIV AS VARCHAR(MAX)) AS VERKRS_PASSIV,
		CAST(VERKRS_TEXT AS VARCHAR(MAX)) AS VERKRS_TEXT,
		CONVERT(varchar(max), VERKS_GILTIG_FOM, 126) AS VERKS_GILTIG_FOM,
		CONVERT(varchar(max), VERKS_GILTIG_TOM, 126) AS VERKS_GILTIG_TOM,
		CAST(VERKS_ID AS VARCHAR(MAX)) AS VERKS_ID,
		CAST(VERKS_ID_TEXT AS VARCHAR(MAX)) AS VERKS_ID_TEXT,
		CAST(VERKS_PASSIV AS VARCHAR(MAX)) AS VERKS_PASSIV,
		CAST(VERKS_TEXT AS VARCHAR(MAX)) AS VERKS_TEXT,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS VGREN_GILTIG_FOM,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS VGREN_GILTIG_TOM,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS VGREN_ID,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS VGREN_ID_TEXT,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS VGREN_PASSIV,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS VGREN_TEXT 
	FROM ftvudp.ftv_400.EK_DIM_OBJ_ENHET ) y

	"""
    return read(query=query, server_url="ftvddbs08.ftv.sll.se")
    