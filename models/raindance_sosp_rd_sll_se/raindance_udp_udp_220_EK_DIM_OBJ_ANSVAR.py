
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSVAR_GILTIG_FOM': 'varchar(max)', 'ANSVAR_GILTIG_TOM': 'varchar(max)', 'ANSVAR_ID': 'varchar(max)', 'ANSVAR_ID_TEXT': 'varchar(max)', 'ANSVAR_PASSIV': 'varchar(max)', 'ANSVAR_TEXT': 'varchar(max)', 'VKO_GILTIG_FOM': 'varchar(max)', 'VKO_GILTIG_TOM': 'varchar(max)', 'VKO_ID': 'varchar(max)', 'VKO_ID_TEXT': 'varchar(max)', 'VKO_PASSIV': 'varchar(max)', 'VKO_TEXT': 'varchar(max)'},
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CONVERT(varchar(max), ANSVAR_GILTIG_FOM, 126) AS ANSVAR_GILTIG_FOM,
		CONVERT(varchar(max), ANSVAR_GILTIG_TOM, 126) AS ANSVAR_GILTIG_TOM,
		CAST(ANSVAR_ID AS VARCHAR(MAX)) AS ANSVAR_ID,
		CAST(ANSVAR_ID_TEXT AS VARCHAR(MAX)) AS ANSVAR_ID_TEXT,
		CAST(ANSVAR_PASSIV AS VARCHAR(MAX)) AS ANSVAR_PASSIV,
		CAST(ANSVAR_TEXT AS VARCHAR(MAX)) AS ANSVAR_TEXT,
		CONVERT(varchar(max), VKO_GILTIG_FOM, 126) AS VKO_GILTIG_FOM,
		CONVERT(varchar(max), VKO_GILTIG_TOM, 126) AS VKO_GILTIG_TOM,
		CAST(VKO_ID AS VARCHAR(MAX)) AS VKO_ID,
		CAST(VKO_ID_TEXT AS VARCHAR(MAX)) AS VKO_ID_TEXT,
		CAST(VKO_PASSIV AS VARCHAR(MAX)) AS VKO_PASSIV,
		CAST(VKO_TEXT AS VARCHAR(MAX)) AS VKO_TEXT 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_ANSVAR ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    