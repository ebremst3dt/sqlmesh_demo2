
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Yrkeskategorier i kassan. En uppsättning koder per företag.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BillingCareProviderCode': 'varchar(max)', 'BillingCareProviderID': 'varchar(max)', 'CompanyID': 'varchar(max)', 'IsDoctor': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'BillingCareProviderID': "{'title_ui': 'Id', 'description': None}", 'BillingCareProviderCode': "{'title_ui': 'Kod', 'description': None}", 'CompanyID': "{'title_ui': 'Kundkoder', 'description': None}", 'Name': "{'title_ui': 'Beskrivning', 'description': None}", 'IsDoctor': "{'title_ui': 'Läkare', 'description': 'Yrkeskategorin är läkare'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['BillingCareProviderID', 'CompanyID']
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
		CAST([BillingCareProviderCode] AS VARCHAR(MAX)) AS [BillingCareProviderCode],
		CAST([BillingCareProviderID] AS VARCHAR(MAX)) AS [BillingCareProviderID],
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([IsDoctor] AS VARCHAR(MAX)) AS [IsDoctor],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_BillingCareProviders_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    