
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ENH_GILTIG_FOM': 'varchar(max)', 'ENH_GILTIG_TOM': 'varchar(max)', 'ENH_ID': 'varchar(max)', 'ENH_ID_TEXT': 'varchar(max)', 'ENH_PASSIV': 'varchar(max)', 'ENH_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'NSO_GILTIG_FOM': 'varchar(max)', 'NSO_GILTIG_TOM': 'varchar(max)', 'NSO_ID': 'varchar(max)', 'NSO_ID_TEXT': 'varchar(max)', 'NSO_PASSIV': 'varchar(max)', 'NSO_TEXT': 'varchar(max)', 'RE_GILTIG_FOM': 'varchar(max)', 'RE_GILTIG_TOM': 'varchar(max)', 'RE_ID': 'varchar(max)', 'RE_ID_TEXT': 'varchar(max)', 'RE_PASSIV': 'varchar(max)', 'RE_TEXT': 'varchar(max)', 'SA_GILTIG_FOM': 'varchar(max)', 'SA_GILTIG_TOM': 'varchar(max)', 'SA_ID': 'varchar(max)', 'SA_ID_TEXT': 'varchar(max)', 'SA_PASSIV': 'varchar(max)', 'SA_TEXT': 'varchar(max)', 'VG_GILTIG_FOM': 'varchar(max)', 'VG_GILTIG_TOM': 'varchar(max)', 'VG_ID': 'varchar(max)', 'VG_ID_TEXT': 'varchar(max)', 'VG_PASSIV': 'varchar(max)', 'VG_TEXT': 'varchar(max)', 'VT_GILTIG_FOM': 'varchar(max)', 'VT_GILTIG_TOM': 'varchar(max)', 'VT_ID': 'varchar(max)', 'VT_ID_TEXT': 'varchar(max)', 'VT_PASSIV': 'varchar(max)', 'VT_TEXT': 'varchar(max)'},
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS ENH_GILTIG_FOM,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS ENH_GILTIG_TOM,
		CAST(ENH_ID AS VARCHAR(MAX)) AS ENH_ID,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS ENH_ID_TEXT,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS ENH_PASSIV,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS ENH_TEXT,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), NSO_GILTIG_FOM, 126) AS NSO_GILTIG_FOM,
		CONVERT(varchar(max), NSO_GILTIG_TOM, 126) AS NSO_GILTIG_TOM,
		CAST(NSO_ID AS VARCHAR(MAX)) AS NSO_ID,
		CAST(NSO_ID_TEXT AS VARCHAR(MAX)) AS NSO_ID_TEXT,
		CAST(NSO_PASSIV AS VARCHAR(MAX)) AS NSO_PASSIV,
		CAST(NSO_TEXT AS VARCHAR(MAX)) AS NSO_TEXT,
		CONVERT(varchar(max), RE_GILTIG_FOM, 126) AS RE_GILTIG_FOM,
		CONVERT(varchar(max), RE_GILTIG_TOM, 126) AS RE_GILTIG_TOM,
		CAST(RE_ID AS VARCHAR(MAX)) AS RE_ID,
		CAST(RE_ID_TEXT AS VARCHAR(MAX)) AS RE_ID_TEXT,
		CAST(RE_PASSIV AS VARCHAR(MAX)) AS RE_PASSIV,
		CAST(RE_TEXT AS VARCHAR(MAX)) AS RE_TEXT,
		CONVERT(varchar(max), SA_GILTIG_FOM, 126) AS SA_GILTIG_FOM,
		CONVERT(varchar(max), SA_GILTIG_TOM, 126) AS SA_GILTIG_TOM,
		CAST(SA_ID AS VARCHAR(MAX)) AS SA_ID,
		CAST(SA_ID_TEXT AS VARCHAR(MAX)) AS SA_ID_TEXT,
		CAST(SA_PASSIV AS VARCHAR(MAX)) AS SA_PASSIV,
		CAST(SA_TEXT AS VARCHAR(MAX)) AS SA_TEXT,
		CONVERT(varchar(max), VG_GILTIG_FOM, 126) AS VG_GILTIG_FOM,
		CONVERT(varchar(max), VG_GILTIG_TOM, 126) AS VG_GILTIG_TOM,
		CAST(VG_ID AS VARCHAR(MAX)) AS VG_ID,
		CAST(VG_ID_TEXT AS VARCHAR(MAX)) AS VG_ID_TEXT,
		CAST(VG_PASSIV AS VARCHAR(MAX)) AS VG_PASSIV,
		CAST(VG_TEXT AS VARCHAR(MAX)) AS VG_TEXT,
		CONVERT(varchar(max), VT_GILTIG_FOM, 126) AS VT_GILTIG_FOM,
		CONVERT(varchar(max), VT_GILTIG_TOM, 126) AS VT_GILTIG_TOM,
		CAST(VT_ID AS VARCHAR(MAX)) AS VT_ID,
		CAST(VT_ID_TEXT AS VARCHAR(MAX)) AS VT_ID_TEXT,
		CAST(VT_PASSIV AS VARCHAR(MAX)) AS VT_PASSIV,
		CAST(VT_TEXT AS VARCHAR(MAX)) AS VT_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    