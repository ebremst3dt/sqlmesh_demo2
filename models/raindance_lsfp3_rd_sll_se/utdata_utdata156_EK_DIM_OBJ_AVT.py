
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVT_GILTIG_FOM': 'varchar(max)', 'AVT_GILTIG_TOM': 'varchar(max)', 'AVT_ID': 'varchar(max)', 'AVT_ID_TEXT': 'varchar(max)', 'AVT_PASSIV': 'varchar(max)', 'AVT_TEXT': 'varchar(max)', 'SHA_GILTIG_FOM': 'varchar(max)', 'SHA_GILTIG_TOM': 'varchar(max)', 'SHA_ID': 'varchar(max)', 'SHA_ID_TEXT': 'varchar(max)', 'SHA_PASSIV': 'varchar(max)', 'SHA_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata156' as _source,
		CONVERT(varchar(max), AVT_GILTIG_FOM, 126) AS AVT_GILTIG_FOM,
		CONVERT(varchar(max), AVT_GILTIG_TOM, 126) AS AVT_GILTIG_TOM,
		CAST(AVT_ID AS VARCHAR(MAX)) AS AVT_ID,
		CAST(AVT_ID_TEXT AS VARCHAR(MAX)) AS AVT_ID_TEXT,
		CAST(AVT_PASSIV AS VARCHAR(MAX)) AS AVT_PASSIV,
		CAST(AVT_TEXT AS VARCHAR(MAX)) AS AVT_TEXT,
		CONVERT(varchar(max), SHA_GILTIG_FOM, 126) AS SHA_GILTIG_FOM,
		CONVERT(varchar(max), SHA_GILTIG_TOM, 126) AS SHA_GILTIG_TOM,
		CAST(SHA_ID AS VARCHAR(MAX)) AS SHA_ID,
		CAST(SHA_ID_TEXT AS VARCHAR(MAX)) AS SHA_ID_TEXT,
		CAST(SHA_PASSIV AS VARCHAR(MAX)) AS SHA_PASSIV,
		CAST(SHA_TEXT AS VARCHAR(MAX)) AS SHA_TEXT 
	FROM utdata.utdata156.EK_DIM_OBJ_AVT ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    