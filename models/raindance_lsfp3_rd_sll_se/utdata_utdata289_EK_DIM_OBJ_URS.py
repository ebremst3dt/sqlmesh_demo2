
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'URS_GILTIG_FOM': 'varchar(max)', 'URS_GILTIG_TOM': 'varchar(max)', 'URS_ID': 'varchar(max)', 'URS_ID_TEXT': 'varchar(max)', 'URS_PASSIV': 'varchar(max)', 'URS_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata289' as _source,
		CONVERT(varchar(max), URS_GILTIG_FOM, 126) AS URS_GILTIG_FOM,
		CONVERT(varchar(max), URS_GILTIG_TOM, 126) AS URS_GILTIG_TOM,
		CAST(URS_ID AS VARCHAR(MAX)) AS URS_ID,
		CAST(URS_ID_TEXT AS VARCHAR(MAX)) AS URS_ID_TEXT,
		CAST(URS_PASSIV AS VARCHAR(MAX)) AS URS_PASSIV,
		CAST(URS_TEXT AS VARCHAR(MAX)) AS URS_TEXT 
	FROM utdata.utdata289.EK_DIM_OBJ_URS ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    