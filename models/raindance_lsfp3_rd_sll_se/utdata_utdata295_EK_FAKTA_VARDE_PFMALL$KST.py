
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'KST_ID': 'varchar(max)', 'MALLID_ID': 'varchar(max)', 'RAK_ID': 'varchar(max)'},
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
    query = """
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$KST

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    