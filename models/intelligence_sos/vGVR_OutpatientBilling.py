
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FileName': 'varchar(max)', 'KOKSCode': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductType': 'varchar(max)', 'ProductValue': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={},
    kind=dict(
        name=ModelKindName.FULL
    ),
    cron="@daily",
    start=start,
    enabled=True
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
 		CAST(CAST(TimestampRead AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST([FileName] AS VARCHAR(MAX)) AS [FileName],
		CAST([KOKSCode] AS VARCHAR(MAX)) AS [KOKSCode],
		CAST([ProductCode] AS VARCHAR(MAX)) AS [ProductCode],
		CAST([ProductType] AS VARCHAR(MAX)) AS [ProductType],
		CAST([ProductValue] AS VARCHAR(MAX)) AS [ProductValue],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransactionID] AS VARCHAR(MAX)) AS [TransactionID] 
	FROM Intelligence.viewreader.vGVR_OutpatientBilling) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    