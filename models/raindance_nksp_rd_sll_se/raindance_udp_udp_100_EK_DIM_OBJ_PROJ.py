
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PROGR_GILTIG_FOM': 'varchar(max)', 'PROGR_GILTIG_TOM': 'varchar(max)', 'PROGR_ID': 'varchar(max)', 'PROGR_ID_TEXT': 'varchar(max)', 'PROGR_PASSIV': 'varchar(max)', 'PROGR_TEXT': 'varchar(max)', 'PROJ_GILTIG_FOM': 'varchar(max)', 'PROJ_GILTIG_TOM': 'varchar(max)', 'PROJ_ID': 'varchar(max)', 'PROJ_ID_TEXT': 'varchar(max)', 'PROJ_PASSIV': 'varchar(max)', 'PROJ_TEXT': 'varchar(max)', 'PROTOT_GILTIG_FOM': 'varchar(max)', 'PROTOT_GILTIG_TOM': 'varchar(max)', 'PROTOT_ID': 'varchar(max)', 'PROTOT_ID_TEXT': 'varchar(max)', 'PROTOT_PASSIV': 'varchar(max)', 'PROTOT_TEXT': 'varchar(max)'},
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
		'nksp_rd_sll_se_raindance_udp_udp_100' as _source,
		CONVERT(varchar(max), PROGR_GILTIG_FOM, 126) AS PROGR_GILTIG_FOM,
		CONVERT(varchar(max), PROGR_GILTIG_TOM, 126) AS PROGR_GILTIG_TOM,
		CAST(PROGR_ID AS VARCHAR(MAX)) AS PROGR_ID,
		CAST(PROGR_ID_TEXT AS VARCHAR(MAX)) AS PROGR_ID_TEXT,
		CAST(PROGR_PASSIV AS VARCHAR(MAX)) AS PROGR_PASSIV,
		CAST(PROGR_TEXT AS VARCHAR(MAX)) AS PROGR_TEXT,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS PROJ_GILTIG_FOM,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS PROJ_GILTIG_TOM,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS PROJ_ID_TEXT,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS PROJ_PASSIV,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS PROJ_TEXT,
		CONVERT(varchar(max), PROTOT_GILTIG_FOM, 126) AS PROTOT_GILTIG_FOM,
		CONVERT(varchar(max), PROTOT_GILTIG_TOM, 126) AS PROTOT_GILTIG_TOM,
		CAST(PROTOT_ID AS VARCHAR(MAX)) AS PROTOT_ID,
		CAST(PROTOT_ID_TEXT AS VARCHAR(MAX)) AS PROTOT_ID_TEXT,
		CAST(PROTOT_PASSIV AS VARCHAR(MAX)) AS PROTOT_PASSIV,
		CAST(PROTOT_TEXT AS VARCHAR(MAX)) AS PROTOT_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_PROJ ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    