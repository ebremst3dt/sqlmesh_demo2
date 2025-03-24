
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Journalöppningar av batch-processer som körs på servern",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProcessName': 'varchar(max)', 'TimestampOpened': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Den journal som öppnades'}", 'TimestampOpened': "{'title_ui': None, 'description': 'Tidpunkt för journalöppning'}", 'ProcessName': "{'title_ui': None, 'description': 'Namn på batch-processen som öppnade journalen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ProcessName] AS VARCHAR(MAX)) AS [ProcessName],
		CONVERT(varchar(max), [TimestampOpened], 126) AS [TimestampOpened],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vRecordLogs_Processes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    