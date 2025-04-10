
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Metadata för teckningar. Teckningar används för att rita bilder och lagra i journalen eller som bifogade teckningar till labbeställningar.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BackgroundDocumentID': 'varchar(max)', 'BackgroundName': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'PredefinedBackgroundID': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None]}}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Senast ändrad på', 'description': None}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'Datum då teckning skapades'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Användaren som skapade den första versionen av dokumentet'}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'EventDate': "{'title_ui': 'Händelsedatum', 'description': 'Det datum teckningen avser'}", 'EventTime': "{'title_ui': 'Händelsedatum', 'description': 'Den tid teckningen avser'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar till teckningen.'}", 'BackgroundDocumentID': "{'title_ui': None, 'description': 'Id på journaldokument med bakgrundsbild. Om fördefinerad bakgrund använts: se kolumn PredefinedBackgroundID'}", 'PredefinedBackgroundID': "{'title_ui': None, 'description': 'Id på systemgemensam bakgrund. Om bild från jounalen använts: se BackgroundDocumentID'}", 'BackgroundName': "{'title_ui': 'Bakgrund', 'description': 'Namn på bakgrund som används'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BackgroundDocumentID] AS VARCHAR(MAX)) AS [BackgroundDocumentID],
		CAST([BackgroundName] AS VARCHAR(MAX)) AS [BackgroundName],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [EventDate], 126) AS [EventDate],
		CONVERT(varchar(max), [EventTime], 126) AS [EventTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PredefinedBackgroundID] AS VARCHAR(MAX)) AS [PredefinedBackgroundID],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vSketches) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    