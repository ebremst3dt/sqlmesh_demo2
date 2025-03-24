
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassahändelser/kassastatus från dagliga filer. En del av de senaste kassahändelserna kopieras över från en dags kassastatusfil till nästa dags, utom stängning av kassa. Det enda som skiljer dessa kopior från varandra och originalet är vilken fil de lästs från av Intelligence.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompanyGroup': 'varchar(max)', 'Counter': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventID': 'varchar(max)', 'EventTime': 'varchar(max)', 'FileNameDate': 'varchar(max)', 'InternalCounterCode': 'varchar(max)', 'ReportDate': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UserID': 'varchar(max)'},
    column_descriptions={'InternalCounterCode': "{'title_ui': None, 'description': 'Kundgrupp och kassakod slås ihop internt för att skapa unik identifierare för kassor'}", 'EventID': "{'title_ui': 'Läge', 'description': {'break': [None, None, None, None]}}", 'EventDate': "{'title_ui': 'Datum', 'description': 'Datum för kassahändelse'}", 'EventTime': "{'title_ui': 'Tid', 'description': 'Tid för kassahändelse'}", 'UserID': "{'title_ui': 'Användarnamn', 'description': 'Användarens PID'}", 'FileNameDate': "{'title_ui': None, 'description': 'Datumet hämtas från kassafilnamnet. Kolumnen existerar bara för att separera annars identiska kassahändelser som kopierats över till nästa dags fil.'}", 'ReportDate': "{'title_ui': 'Kassarapportdatum', 'description': 'Senaste kassarapportdatum'}", 'CompanyGroup': "{'title_ui': 'Kundgrupp', 'description': None}", 'Counter': "{'title_ui': 'Kassa', 'description': 'Kassakod'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
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
		CAST([CompanyGroup] AS VARCHAR(MAX)) AS [CompanyGroup],
		CAST([Counter] AS VARCHAR(MAX)) AS [Counter],
		CONVERT(varchar(max), [EventDate], 126) AS [EventDate],
		CAST([EventID] AS VARCHAR(MAX)) AS [EventID],
		CONVERT(varchar(max), [EventTime], 126) AS [EventTime],
		CONVERT(varchar(max), [FileNameDate], 126) AS [FileNameDate],
		CAST([InternalCounterCode] AS VARCHAR(MAX)) AS [InternalCounterCode],
		CONVERT(varchar(max), [ReportDate], 126) AS [ReportDate],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([UserID] AS VARCHAR(MAX)) AS [UserID] 
	FROM Intelligence.viewreader.vCounterEvents) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    