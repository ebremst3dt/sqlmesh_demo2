
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'URSP_GILTIG_FOM': 'varchar(max)', 'URSP_GILTIG_TOM': 'varchar(max)', 'URSP_ID': 'varchar(max)', 'URSP_ID_TEXT': 'varchar(max)', 'URSP_PASSIV': 'varchar(max)', 'URSP_TEXT': 'varchar(max)'},
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
		'stsp_rd_sll_se_stsudp_udp_858' as _source,
		CONVERT(varchar(max), URSP_GILTIG_FOM, 126) AS ursp_giltig_fom,
		CONVERT(varchar(max), URSP_GILTIG_TOM, 126) AS ursp_giltig_tom,
		CAST(URSP_ID AS VARCHAR(MAX)) AS ursp_id,
		CAST(URSP_ID_TEXT AS VARCHAR(MAX)) AS ursp_id_text,
		CAST(URSP_PASSIV AS VARCHAR(MAX)) AS ursp_passiv,
		CAST(URSP_TEXT AS VARCHAR(MAX)) AS ursp_text 
	FROM stsudp.udp_858.EK_DIM_OBJ_URSP ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    