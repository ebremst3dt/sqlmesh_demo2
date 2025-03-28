
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVD_GILTIG_FOM': 'varchar(max)', 'AVD_GILTIG_TOM': 'varchar(max)', 'AVD_ID': 'varchar(max)', 'AVD_ID_TEXT': 'varchar(max)', 'AVD_PASSIV': 'varchar(max)', 'AVD_TEXT': 'varchar(max)', 'RAM_GILTIG_FOM': 'varchar(max)', 'RAM_GILTIG_TOM': 'varchar(max)', 'RAM_ID': 'varchar(max)', 'RAM_ID_TEXT': 'varchar(max)', 'RAM_PASSIV': 'varchar(max)', 'RAM_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata150' as _source,
		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS AVD_GILTIG_FOM,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS AVD_GILTIG_TOM,
		CAST(AVD_ID AS VARCHAR(MAX)) AS AVD_ID,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS AVD_ID_TEXT,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS AVD_PASSIV,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS AVD_TEXT,
		CONVERT(varchar(max), RAM_GILTIG_FOM, 126) AS RAM_GILTIG_FOM,
		CONVERT(varchar(max), RAM_GILTIG_TOM, 126) AS RAM_GILTIG_TOM,
		CAST(RAM_ID AS VARCHAR(MAX)) AS RAM_ID,
		CAST(RAM_ID_TEXT AS VARCHAR(MAX)) AS RAM_ID_TEXT,
		CAST(RAM_PASSIV AS VARCHAR(MAX)) AS RAM_PASSIV,
		CAST(RAM_TEXT AS VARCHAR(MAX)) AS RAM_TEXT 
	FROM utdata.utdata150.EK_DIM_OBJ_AVD ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    