
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Produkter för DRG. Kommer ursprungligen från kodservern. Endast senaste versionen av varje produkt.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Description': 'varchar(max)', 'Name': 'varchar(max)', 'OutlierLimit': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductType': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'Weight': 'varchar(max)'},
    column_descriptions={'ProductType': "{'title_ui': None, 'description': None}", 'ProductCode': "{'title_ui': None, 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'Description': "{'title_ui': None, 'description': None}", 'Weight': "{'title_ui': None, 'description': None}", 'OutlierLimit': "{'title_ui': None, 'description': 'Ytterfallsgräns'}", 'ValidThroughDate': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['ProductCode', 'ProductType']
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
		CAST([Description] AS VARCHAR(MAX)) AS [Description],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([OutlierLimit] AS VARCHAR(MAX)) AS [OutlierLimit],
		CAST([ProductCode] AS VARCHAR(MAX)) AS [ProductCode],
		CAST([ProductType] AS VARCHAR(MAX)) AS [ProductType],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate],
		CAST([Weight] AS VARCHAR(MAX)) AS [Weight] 
	FROM Intelligence.viewreader.vCodes_ProductCodes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    