
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KGRR_GILTIG_FOM': 'varchar(max)', 'KGRR_GILTIG_TOM': 'varchar(max)', 'KGRR_ID': 'varchar(max)', 'KGRR_ID_TEXT': 'varchar(max)', 'KGRR_PASSIV': 'varchar(max)', 'KGRR_TEXT': 'varchar(max)', 'KKLRR_GILTIG_FOM': 'varchar(max)', 'KKLRR_GILTIG_TOM': 'varchar(max)', 'KKLRR_ID': 'varchar(max)', 'KKLRR_ID_TEXT': 'varchar(max)', 'KKLRR_PASSIV': 'varchar(max)', 'KKLRR_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), KGRR_GILTIG_FOM, 126) AS KGRR_GILTIG_FOM,
		CONVERT(varchar(max), KGRR_GILTIG_TOM, 126) AS KGRR_GILTIG_TOM,
		CAST(KGRR_ID AS VARCHAR(MAX)) AS KGRR_ID,
		CAST(KGRR_ID_TEXT AS VARCHAR(MAX)) AS KGRR_ID_TEXT,
		CAST(KGRR_PASSIV AS VARCHAR(MAX)) AS KGRR_PASSIV,
		CAST(KGRR_TEXT AS VARCHAR(MAX)) AS KGRR_TEXT,
		CONVERT(varchar(max), KKLRR_GILTIG_FOM, 126) AS KKLRR_GILTIG_FOM,
		CONVERT(varchar(max), KKLRR_GILTIG_TOM, 126) AS KKLRR_GILTIG_TOM,
		CAST(KKLRR_ID AS VARCHAR(MAX)) AS KKLRR_ID,
		CAST(KKLRR_ID_TEXT AS VARCHAR(MAX)) AS KKLRR_ID_TEXT,
		CAST(KKLRR_PASSIV AS VARCHAR(MAX)) AS KKLRR_PASSIV,
		CAST(KKLRR_TEXT AS VARCHAR(MAX)) AS KKLRR_TEXT 
	FROM utdata.utdata298.EK_DIM_OBJ_KGRR ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    