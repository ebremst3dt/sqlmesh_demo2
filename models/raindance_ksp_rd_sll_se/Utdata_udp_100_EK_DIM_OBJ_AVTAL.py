
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVTAL_GILTIG_FOM': 'varchar(max)', 'AVTAL_GILTIG_TOM': 'varchar(max)', 'AVTAL_ID': 'varchar(max)', 'AVTAL_ID_TEXT': 'varchar(max)', 'AVTAL_PASSIV': 'varchar(max)', 'AVTAL_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), AVTAL_GILTIG_FOM, 126) AS AVTAL_GILTIG_FOM,
		CONVERT(varchar(max), AVTAL_GILTIG_TOM, 126) AS AVTAL_GILTIG_TOM,
		CAST(AVTAL_ID AS VARCHAR(MAX)) AS AVTAL_ID,
		CAST(AVTAL_ID_TEXT AS VARCHAR(MAX)) AS AVTAL_ID_TEXT,
		CAST(AVTAL_PASSIV AS VARCHAR(MAX)) AS AVTAL_PASSIV,
		CAST(AVTAL_TEXT AS VARCHAR(MAX)) AS AVTAL_TEXT 
	FROM Utdata.udp_100.EK_DIM_OBJ_AVTAL ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    