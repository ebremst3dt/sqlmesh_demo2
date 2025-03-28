
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEKT_GILTIG_FOM': 'varchar(max)', 'SEKT_GILTIG_TOM': 'varchar(max)', 'SEKT_ID': 'varchar(max)', 'SEKT_ID_TEXT': 'varchar(max)', 'SEKT_PASSIV': 'varchar(max)', 'SEKT_TEXT': 'varchar(max)', 'VERK_GILTIG_FOM': 'varchar(max)', 'VERK_GILTIG_TOM': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERK_ID_TEXT': 'varchar(max)', 'VERK_PASSIV': 'varchar(max)', 'VERK_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)', 'VO_GILTIG_FOM': 'varchar(max)', 'VO_GILTIG_TOM': 'varchar(max)', 'VO_ID': 'varchar(max)', 'VO_ID_TEXT': 'varchar(max)', 'VO_PASSIV': 'varchar(max)', 'VO_TEXT': 'varchar(max)', 'V_GILTIG_FOM': 'varchar(max)', 'V_GILTIG_TOM': 'varchar(max)', 'V_ID': 'varchar(max)', 'V_ID_TEXT': 'varchar(max)', 'V_PASSIV': 'varchar(max)', 'V_TEXT': 'varchar(max)'},
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), SEKT_GILTIG_FOM, 126) AS SEKT_GILTIG_FOM,
		CONVERT(varchar(max), SEKT_GILTIG_TOM, 126) AS SEKT_GILTIG_TOM,
		CAST(SEKT_ID AS VARCHAR(MAX)) AS SEKT_ID,
		CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS SEKT_ID_TEXT,
		CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS SEKT_PASSIV,
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS SEKT_TEXT,
		CONVERT(varchar(max), VERK_GILTIG_FOM, 126) AS VERK_GILTIG_FOM,
		CONVERT(varchar(max), VERK_GILTIG_TOM, 126) AS VERK_GILTIG_TOM,
		CAST(VERK_ID AS VARCHAR(MAX)) AS VERK_ID,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS VERK_ID_TEXT,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS VERK_PASSIV,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS VERK_TEXT,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS VGREN_GILTIG_FOM,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS VGREN_GILTIG_TOM,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS VGREN_ID,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS VGREN_ID_TEXT,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS VGREN_PASSIV,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS VGREN_TEXT,
		CONVERT(varchar(max), VO_GILTIG_FOM, 126) AS VO_GILTIG_FOM,
		CONVERT(varchar(max), VO_GILTIG_TOM, 126) AS VO_GILTIG_TOM,
		CAST(VO_ID AS VARCHAR(MAX)) AS VO_ID,
		CAST(VO_ID_TEXT AS VARCHAR(MAX)) AS VO_ID_TEXT,
		CAST(VO_PASSIV AS VARCHAR(MAX)) AS VO_PASSIV,
		CAST(VO_TEXT AS VARCHAR(MAX)) AS VO_TEXT,
		CONVERT(varchar(max), V_GILTIG_FOM, 126) AS V_GILTIG_FOM,
		CONVERT(varchar(max), V_GILTIG_TOM, 126) AS V_GILTIG_TOM,
		CAST(V_ID AS VARCHAR(MAX)) AS V_ID,
		CAST(V_ID_TEXT AS VARCHAR(MAX)) AS V_ID_TEXT,
		CAST(V_PASSIV AS VARCHAR(MAX)) AS V_PASSIV,
		CAST(V_TEXT AS VARCHAR(MAX)) AS V_TEXT 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    