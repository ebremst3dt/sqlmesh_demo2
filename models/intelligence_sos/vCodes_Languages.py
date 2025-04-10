
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read

    
@model(
    description="Språk",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ISO1Code': 'varchar(max)', 'ISO2Code': 'varchar(max)', 'IsSelectableInPrescriptions': 'varchar(max)', 'LanguageID': 'varchar(max)', 'Name': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'LanguageID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'ISO1Code': "{'title_ui': 'Kod ISO 639-1', 'description': 'Språkets kod enl. ISO 639-1'}", 'ISO2Code': "{'title_ui': 'Kod ISO 639-2', 'description': 'Språkets kod enl. ISO 639-2'}", 'TermID': "{'title_ui': 'Kod i termkatalog', 'description': None}", 'IsSelectableInPrescriptions': "{'title_ui': 'Valbar vid recept', 'description': 'Visad som valbart språk för doseringsanvisning i recept'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ISO1Code] AS VARCHAR(MAX)) AS [ISO1Code],
		CAST([ISO2Code] AS VARCHAR(MAX)) AS [ISO2Code],
		CAST([IsSelectableInPrescriptions] AS VARCHAR(MAX)) AS [IsSelectableInPrescriptions],
		CAST([LanguageID] AS VARCHAR(MAX)) AS [LanguageID],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_Languages) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    