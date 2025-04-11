
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Resultat från undersökningar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AccessionNumber': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExamDateTime': 'varchar(max)', 'MethodCode': 'varchar(max)', 'Organ': 'varchar(max)', 'OrganID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SequenceID': 'varchar(max)', 'Side': 'varchar(max)', 'StatusCode': 'varchar(max)', 'StatusComment': 'varchar(max)', 'StatusEventDateTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'OrganID': "{'title_ui': None, 'description': 'Organkod'}", 'Organ': "{'title_ui': None, 'description': 'Organ i klartext'}", 'Side': "{'title_ui': None, 'description': {'break': None}}", 'ExamDateTime': "{'title_ui': None, 'description': 'Datum och tid då undersökningen genomfördes.'}", 'MethodCode': "{'title_ui': None, 'description': 'Metodkod'}", 'SequenceID': "{'title_ui': None, 'description': 'Sekvens-id för undersökningen'}", 'AccessionNumber': "{'title_ui': None, 'description': 'Id för bild i PACS'}", 'StatusCode': "{'title_ui': 'Status', 'description': 'Statuskod'}", 'StatusEventDateTime': "{'title_ui': 'Tid', 'description': 'Händelsetid för det som statuskoden avser'}", 'StatusComment': "{'title_ui': 'Kommentar', 'description': 'Statuskommentar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID', 'Row', 'Version']
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
		CAST([AccessionNumber] AS VARCHAR(MAX)) AS [AccessionNumber],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [ExamDateTime], 126) AS [ExamDateTime],
		CAST([MethodCode] AS VARCHAR(MAX)) AS [MethodCode],
		CAST([Organ] AS VARCHAR(MAX)) AS [Organ],
		CAST([OrganID] AS VARCHAR(MAX)) AS [OrganID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SequenceID] AS VARCHAR(MAX)) AS [SequenceID],
		CAST([Side] AS VARCHAR(MAX)) AS [Side],
		CAST([StatusCode] AS VARCHAR(MAX)) AS [StatusCode],
		CAST([StatusComment] AS VARCHAR(MAX)) AS [StatusComment],
		CONVERT(varchar(max), [StatusEventDateTime], 126) AS [StatusEventDateTime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vRadiologyRepliesSectra_Exams) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    