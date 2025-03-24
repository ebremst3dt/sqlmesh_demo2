
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Här finns alla utföranden av aktiviteter. Även aktiviteter som makuleras, sätts ut och explicit markeras som Ej utförda lagras som rader här. Använd främmande nyckel till Tasks för att se vilken aktivitet som utfördes. Vissa sorters aktiviteter kan utföras flera gånger.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CompletedTaskID': 'varchar(max)', 'CompletedTaskTimestampCreated': 'varchar(max)', 'ExecutionID': 'varchar(max)', 'IsPerformed': 'varchar(max)', 'PatientID': 'varchar(max)', 'PerformedDate': 'varchar(max)', 'PerformedTime': 'varchar(max)', 'PerformedTimestamp': 'varchar(max)', 'PlannedDate': 'varchar(max)', 'PlannedTime': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När den första versionen av utförandet skapades'}", 'ExecutionID': "{'title_ui': None, 'description': 'Internt id som identifierar detta utförande'}", 'CompletedTaskTimestampCreated': "{'title_ui': None, 'description': 'Tidsstämpel för den aktivitet som utfördes (del av främmande nyckel).'}", 'CompletedTaskID': "{'title_ui': None, 'description': 'Id för den aktivitet som utfördes (del av främmande nyckel).'}", 'TermID': "{'title_ui': None, 'description': 'Termid för den aktivitet som utfördes (del av främmande nyckel).'}", 'IsPerformed': "{'title_ui': 'Utförd', 'description': 'Användaren kan explicit markera en aktivitet som ej utförd och ange en kommentar, då är detta fält falskt.'}", 'PlannedDate': "{'title_ui': 'Planerad tid', 'description': 'Det datum aktiviteten planerades att utföras'}", 'PlannedTime': "{'title_ui': 'Planerad tid', 'description': 'Den tid aktiviteten planerades att utföras'}", 'PerformedDate': "{'title_ui': 'Utförd/Utförd, faktiskt tid', 'description': 'Datum användaren angett att aktiviteten utfördes, sattes ut, makulerades eller markerades som Ej utförd.'}", 'PerformedTime': "{'title_ui': 'Utförd/Utförd, faktiskt tid', 'description': 'Tid användaren angett att aktiviteten utfördes, sattes ut, makulerades eller markerades som Ej utförd.'}", 'PerformedTimestamp': "{'title_ui': None, 'description': 'Tidsstämpel då aktiviteten faktiskt sparades som utförd (sätts av systemet).'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Om en aktivitet markeras som Ej utförd, så kan användaren skriva en kommentar i detta fält'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CompletedTaskID] AS VARCHAR(MAX)) AS [CompletedTaskID],
		CONVERT(varchar(max), [CompletedTaskTimestampCreated], 126) AS [CompletedTaskTimestampCreated],
		CAST([ExecutionID] AS VARCHAR(MAX)) AS [ExecutionID],
		CAST([IsPerformed] AS VARCHAR(MAX)) AS [IsPerformed],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PerformedDate], 126) AS [PerformedDate],
		CONVERT(varchar(max), [PerformedTime], 126) AS [PerformedTime],
		CONVERT(varchar(max), [PerformedTimestamp], 126) AS [PerformedTimestamp],
		CONVERT(varchar(max), [PlannedDate], 126) AS [PlannedDate],
		CONVERT(varchar(max), [PlannedTime], 126) AS [PlannedTime],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vTasks_Executions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    