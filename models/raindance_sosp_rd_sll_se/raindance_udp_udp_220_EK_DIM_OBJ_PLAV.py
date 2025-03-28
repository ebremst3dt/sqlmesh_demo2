
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PLAV_GILTIG_FOM': 'varchar(max)', 'PLAV_GILTIG_TOM': 'varchar(max)', 'PLAV_ID': 'varchar(max)', 'PLAV_ID_TEXT': 'varchar(max)', 'PLAV_PASSIV': 'varchar(max)', 'PLAV_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), PLAV_GILTIG_FOM, 126) AS PLAV_GILTIG_FOM,
		CONVERT(varchar(max), PLAV_GILTIG_TOM, 126) AS PLAV_GILTIG_TOM,
		CAST(PLAV_ID AS VARCHAR(MAX)) AS PLAV_ID,
		CAST(PLAV_ID_TEXT AS VARCHAR(MAX)) AS PLAV_ID_TEXT,
		CAST(PLAV_PASSIV AS VARCHAR(MAX)) AS PLAV_PASSIV,
		CAST(PLAV_TEXT AS VARCHAR(MAX)) AS PLAV_TEXT 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_PLAV ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    