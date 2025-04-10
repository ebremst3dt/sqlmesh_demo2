
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Funktioner som kan kopplas till en användarprofil.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FunctionDescription': 'varchar(max)', 'FunctionID': 'varchar(max)', 'FunctionName': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'FunctionID': "{'title_ui': 'Funktionskod', 'description': 'Funktionskod'}", 'FunctionName': "{'title_ui': 'Funktionsnamn', 'description': 'Funktionens namn'}", 'FunctionDescription': "{'title_ui': 'Funktionsbeskrivning', 'description': 'Funktionens beskrivning'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([FunctionDescription] AS VARCHAR(MAX)) AS [FunctionDescription],
		CAST([FunctionID] AS VARCHAR(MAX)) AS [FunctionID],
		CAST([FunctionName] AS VARCHAR(MAX)) AS [FunctionName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_Functions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    