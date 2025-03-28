
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PLED_GILTIG_FOM': 'varchar(max)', 'PLED_GILTIG_TOM': 'varchar(max)', 'PLED_ID': 'varchar(max)', 'PLED_ID_TEXT': 'varchar(max)', 'PLED_PASSIV': 'varchar(max)', 'PLED_TEXT': 'varchar(max)'},
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CONVERT(varchar(max), PLED_GILTIG_FOM, 126) AS PLED_GILTIG_FOM,
		CONVERT(varchar(max), PLED_GILTIG_TOM, 126) AS PLED_GILTIG_TOM,
		CAST(PLED_ID AS VARCHAR(MAX)) AS PLED_ID,
		CAST(PLED_ID_TEXT AS VARCHAR(MAX)) AS PLED_ID_TEXT,
		CAST(PLED_PASSIV AS VARCHAR(MAX)) AS PLED_PASSIV,
		CAST(PLED_TEXT AS VARCHAR(MAX)) AS PLED_TEXT 
	FROM Utdata.udp_100.EK_DIM_OBJ_PLED ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    