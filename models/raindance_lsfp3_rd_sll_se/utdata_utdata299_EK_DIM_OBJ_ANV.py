
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANV_GILTIG_FOM': 'varchar(max)', 'ANV_GILTIG_TOM': 'varchar(max)', 'ANV_ID': 'varchar(max)', 'ANV_ID_TEXT': 'varchar(max)', 'ANV_PASSIV': 'varchar(max)', 'ANV_TEXT': 'varchar(max)', 'FAKTCE_GILTIG_FOM': 'varchar(max)', 'FAKTCE_GILTIG_TOM': 'varchar(max)', 'FAKTCE_ID': 'varchar(max)', 'FAKTCE_ID_TEXT': 'varchar(max)', 'FAKTCE_PASSIV': 'varchar(max)', 'FAKTCE_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata299' as _source,
		CONVERT(varchar(max), ANV_GILTIG_FOM, 126) AS ANV_GILTIG_FOM,
		CONVERT(varchar(max), ANV_GILTIG_TOM, 126) AS ANV_GILTIG_TOM,
		CAST(ANV_ID AS VARCHAR(MAX)) AS ANV_ID,
		CAST(ANV_ID_TEXT AS VARCHAR(MAX)) AS ANV_ID_TEXT,
		CAST(ANV_PASSIV AS VARCHAR(MAX)) AS ANV_PASSIV,
		CAST(ANV_TEXT AS VARCHAR(MAX)) AS ANV_TEXT,
		CONVERT(varchar(max), FAKTCE_GILTIG_FOM, 126) AS FAKTCE_GILTIG_FOM,
		CONVERT(varchar(max), FAKTCE_GILTIG_TOM, 126) AS FAKTCE_GILTIG_TOM,
		CAST(FAKTCE_ID AS VARCHAR(MAX)) AS FAKTCE_ID,
		CAST(FAKTCE_ID_TEXT AS VARCHAR(MAX)) AS FAKTCE_ID_TEXT,
		CAST(FAKTCE_PASSIV AS VARCHAR(MAX)) AS FAKTCE_PASSIV,
		CAST(FAKTCE_TEXT AS VARCHAR(MAX)) AS FAKTCE_TEXT 
	FROM utdata.utdata299.EK_DIM_OBJ_ANV ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    