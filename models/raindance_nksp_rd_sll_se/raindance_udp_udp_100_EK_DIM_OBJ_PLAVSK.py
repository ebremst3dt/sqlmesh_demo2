
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PLAVSK_GILTIG_FOM': 'varchar(max)', 'PLAVSK_GILTIG_TOM': 'varchar(max)', 'PLAVSK_ID': 'varchar(max)', 'PLAVSK_ID_TEXT': 'varchar(max)', 'PLAVSK_PASSIV': 'varchar(max)', 'PLAVSK_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), PLAVSK_GILTIG_FOM, 126) AS PLAVSK_GILTIG_FOM,
		CONVERT(varchar(max), PLAVSK_GILTIG_TOM, 126) AS PLAVSK_GILTIG_TOM,
		CAST(PLAVSK_ID AS VARCHAR(MAX)) AS PLAVSK_ID,
		CAST(PLAVSK_ID_TEXT AS VARCHAR(MAX)) AS PLAVSK_ID_TEXT,
		CAST(PLAVSK_PASSIV AS VARCHAR(MAX)) AS PLAVSK_PASSIV,
		CAST(PLAVSK_TEXT AS VARCHAR(MAX)) AS PLAVSK_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_PLAVSK ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    