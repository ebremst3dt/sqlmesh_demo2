
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Triage vitalparametrar (kopia på mätvärde). Mätvärden finns även i CaseNotes_Measurements och Measurements.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'MeasurementTimestampSigned': 'varchar(max)', 'PatientID': 'varchar(max)', 'PriorityID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TriageMeasurementUUID': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TermID': "{'title_ui': 'Term', 'description': 'Kod för term'}", 'Value': "{'title_ui': 'Mätvärde', 'description': 'Kopia på mätvärde'}", 'PriorityID': "{'title_ui': None, 'description': 'Avgör var kryssen sätts'}", 'EventDate': "{'title_ui': 'Händelsetid', 'description': 'Det datum mätvärdet avser (kopia på mätvärde)'}", 'EventTime': "{'title_ui': 'Händelsetid', 'description': 'Den tid mätvärdet avser (kopia på mätvärde)'}", 'MeasurementTimestampSigned': "{'title_ui': None, 'description': 'Signeringstidpunkt för kopplat mätvärde'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Fritextkommentar'}", 'TriageMeasurementUUID': "{'title_ui': None, 'description': 'UUID för kopplat mätvärde'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID', 'TermID', 'Version']
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
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [EventDate], 126) AS [EventDate],
		CONVERT(varchar(max), [EventTime], 126) AS [EventTime],
		CONVERT(varchar(max), [MeasurementTimestampSigned], 126) AS [MeasurementTimestampSigned],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PriorityID] AS VARCHAR(MAX)) AS [PriorityID],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TriageMeasurementUUID] AS VARCHAR(MAX)) AS [TriageMeasurementUUID],
		CAST([Value] AS VARCHAR(MAX)) AS [Value],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vEmergency_TriageVitalMeasurements) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    