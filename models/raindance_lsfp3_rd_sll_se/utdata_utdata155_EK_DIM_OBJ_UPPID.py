
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'UPPID_GILTIG_FOM': 'varchar(max)', 'UPPID_GILTIG_TOM': 'varchar(max)', 'UPPID_ID': 'varchar(max)', 'UPPID_ID_TEXT': 'varchar(max)', 'UPPID_PASSIV': 'varchar(max)', 'UPPID_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata155' as _source,
		CONVERT(varchar(max), UPPID_GILTIG_FOM, 126) AS UPPID_GILTIG_FOM,
		CONVERT(varchar(max), UPPID_GILTIG_TOM, 126) AS UPPID_GILTIG_TOM,
		CAST(UPPID_ID AS VARCHAR(MAX)) AS UPPID_ID,
		CAST(UPPID_ID_TEXT AS VARCHAR(MAX)) AS UPPID_ID_TEXT,
		CAST(UPPID_PASSIV AS VARCHAR(MAX)) AS UPPID_PASSIV,
		CAST(UPPID_TEXT AS VARCHAR(MAX)) AS UPPID_TEXT 
	FROM utdata.utdata155.EK_DIM_OBJ_UPPID ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    