
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'IMOT_GILTIG_FOM': 'varchar(max)', 'IMOT_GILTIG_TOM': 'varchar(max)', 'IMOT_ID': 'varchar(max)', 'IMOT_ID_TEXT': 'varchar(max)', 'IMOT_PASSIV': 'varchar(max)', 'IMOT_TEXT': 'varchar(max)', 'REIMOT_GILTIG_FOM': 'varchar(max)', 'REIMOT_GILTIG_TOM': 'varchar(max)', 'REIMOT_ID': 'varchar(max)', 'REIMOT_ID_TEXT': 'varchar(max)', 'REIMOT_PASSIV': 'varchar(max)', 'REIMOT_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), IMOT_GILTIG_FOM, 126) AS IMOT_GILTIG_FOM,
		CONVERT(varchar(max), IMOT_GILTIG_TOM, 126) AS IMOT_GILTIG_TOM,
		CAST(IMOT_ID AS VARCHAR(MAX)) AS IMOT_ID,
		CAST(IMOT_ID_TEXT AS VARCHAR(MAX)) AS IMOT_ID_TEXT,
		CAST(IMOT_PASSIV AS VARCHAR(MAX)) AS IMOT_PASSIV,
		CAST(IMOT_TEXT AS VARCHAR(MAX)) AS IMOT_TEXT,
		CONVERT(varchar(max), REIMOT_GILTIG_FOM, 126) AS REIMOT_GILTIG_FOM,
		CONVERT(varchar(max), REIMOT_GILTIG_TOM, 126) AS REIMOT_GILTIG_TOM,
		CAST(REIMOT_ID AS VARCHAR(MAX)) AS REIMOT_ID,
		CAST(REIMOT_ID_TEXT AS VARCHAR(MAX)) AS REIMOT_ID_TEXT,
		CAST(REIMOT_PASSIV AS VARCHAR(MAX)) AS REIMOT_PASSIV,
		CAST(REIMOT_TEXT AS VARCHAR(MAX)) AS REIMOT_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_IMOT ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    