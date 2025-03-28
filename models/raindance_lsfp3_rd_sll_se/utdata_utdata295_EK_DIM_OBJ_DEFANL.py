
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DEFANL_GILTIG_FOM': 'varchar(max)', 'DEFANL_GILTIG_TOM': 'varchar(max)', 'DEFANL_ID': 'varchar(max)', 'DEFANL_ID_TEXT': 'varchar(max)', 'DEFANL_PASSIV': 'varchar(max)', 'DEFANL_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CONVERT(varchar(max), DEFANL_GILTIG_FOM, 126) AS DEFANL_GILTIG_FOM,
		CONVERT(varchar(max), DEFANL_GILTIG_TOM, 126) AS DEFANL_GILTIG_TOM,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS DEFANL_ID,
		CAST(DEFANL_ID_TEXT AS VARCHAR(MAX)) AS DEFANL_ID_TEXT,
		CAST(DEFANL_PASSIV AS VARCHAR(MAX)) AS DEFANL_PASSIV,
		CAST(DEFANL_TEXT AS VARCHAR(MAX)) AS DEFANL_TEXT 
	FROM utdata.utdata295.EK_DIM_OBJ_DEFANL ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    