
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="En resurs kan vara t.ex. en person, ett rum, eller en befattning. En bokning görs för en viss resurs ett visst datum på en viss tid. Resurser lagras inte med versionshantering, däremot tas resurser aldrig bort. Varje vårdenhet definierar själv sina resurser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareProviderID': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'Name': 'varchar(max)', 'ResourceID': 'varchar(max)', 'ResourceTypeID': 'varchar(max)', 'ScheduleMode': 'varchar(max)', 'ShareStatus': 'varchar(max)', 'ShortName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'ResourceID': "{'title_ui': None, 'description': 'Resursens id'}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'ShortName': "{'title_ui': 'Kortnamn', 'description': None}", 'ResourceTypeID': "{'title_ui': 'Typ', 'description': 'Person, rum, befattning etc'}", 'ValidFromDate': "{'title_ui': 'Giltig from', 'description': 'Om kolumnen är NULL anses resursen giltig så länge även Giltig tom stämmer. Ogilitiga resurser kan inte få nya bokningar, kan t.ex vara en person som slutat eller flyttat till en annan enhet.'}", 'ValidThroughDate': "{'title_ui': 'Giltig tom', 'description': 'Om kolumnen är NULL anses resursen giltig så länge även Giltig from stämmer.'}", 'CareProviderID': "{'title_ui': 'Vårdgivarkod', 'description': 'Vårdgivarkoden används i kassan'}", 'ShareStatus': "{'title_ui': 'Utdelat', 'description': {'break': [None, None, None, None]}}", 'ScheduleMode': "{'title_ui': 'Inget schema', 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['CareUnitID', 'ResourceID']
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
		CAST([CareProviderID] AS VARCHAR(MAX)) AS [CareProviderID],
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([ResourceID] AS VARCHAR(MAX)) AS [ResourceID],
		CAST([ResourceTypeID] AS VARCHAR(MAX)) AS [ResourceTypeID],
		CAST([ScheduleMode] AS VARCHAR(MAX)) AS [ScheduleMode],
		CAST([ShareStatus] AS VARCHAR(MAX)) AS [ShareStatus],
		CAST([ShortName] AS VARCHAR(MAX)) AS [ShortName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_Resources) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    