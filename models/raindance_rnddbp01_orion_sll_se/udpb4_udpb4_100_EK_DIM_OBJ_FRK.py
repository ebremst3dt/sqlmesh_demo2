
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRG_GILTIG_FOM': 'varchar(max)', 'FRG_GILTIG_TOM': 'varchar(max)', 'FRG_ID': 'varchar(max)', 'FRG_ID_TEXT': 'varchar(max)', 'FRG_PASSIV': 'varchar(max)', 'FRG_TEXT': 'varchar(max)', 'FRK_GILTIG_FOM': 'varchar(max)', 'FRK_GILTIG_TOM': 'varchar(max)', 'FRK_ID': 'varchar(max)', 'FRK_ID_TEXT': 'varchar(max)', 'FRK_PASSIV': 'varchar(max)', 'FRK_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), FRG_GILTIG_FOM, 126) AS FRG_GILTIG_FOM,
		CONVERT(varchar(max), FRG_GILTIG_TOM, 126) AS FRG_GILTIG_TOM,
		CAST(FRG_ID AS VARCHAR(MAX)) AS FRG_ID,
		CAST(FRG_ID_TEXT AS VARCHAR(MAX)) AS FRG_ID_TEXT,
		CAST(FRG_PASSIV AS VARCHAR(MAX)) AS FRG_PASSIV,
		CAST(FRG_TEXT AS VARCHAR(MAX)) AS FRG_TEXT,
		CONVERT(varchar(max), FRK_GILTIG_FOM, 126) AS FRK_GILTIG_FOM,
		CONVERT(varchar(max), FRK_GILTIG_TOM, 126) AS FRK_GILTIG_TOM,
		CAST(FRK_ID AS VARCHAR(MAX)) AS FRK_ID,
		CAST(FRK_ID_TEXT AS VARCHAR(MAX)) AS FRK_ID_TEXT,
		CAST(FRK_PASSIV AS VARCHAR(MAX)) AS FRK_PASSIV,
		CAST(FRK_TEXT AS VARCHAR(MAX)) AS FRK_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_FRK ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    