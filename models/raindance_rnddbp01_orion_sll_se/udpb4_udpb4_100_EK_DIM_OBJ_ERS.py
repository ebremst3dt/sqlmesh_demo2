
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ERS_GILTIG_FOM': 'varchar(max)', 'ERS_GILTIG_TOM': 'varchar(max)', 'ERS_ID': 'varchar(max)', 'ERS_ID_TEXT': 'varchar(max)', 'ERS_PASSIV': 'varchar(max)', 'ERS_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), ERS_GILTIG_FOM, 126) AS ERS_GILTIG_FOM,
		CONVERT(varchar(max), ERS_GILTIG_TOM, 126) AS ERS_GILTIG_TOM,
		CAST(ERS_ID AS VARCHAR(MAX)) AS ERS_ID,
		CAST(ERS_ID_TEXT AS VARCHAR(MAX)) AS ERS_ID_TEXT,
		CAST(ERS_PASSIV AS VARCHAR(MAX)) AS ERS_PASSIV,
		CAST(ERS_TEXT AS VARCHAR(MAX)) AS ERS_TEXT 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_ERS ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    