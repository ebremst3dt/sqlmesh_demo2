
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Digital diktering. En sparad ljudfil kan ej modifieras, däremot kan vissa kolumner i dokumenttypen modifieras: PriorityID, LinkedDocumentTemplateID, SavedAtCareUnitID. Dokumenttypen versionshanteras ej.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUser': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'HistoryStatusID': 'varchar(max)', 'LinkedDocumentTemplateID': 'varchar(max)', 'LinkedDocumentTypeID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PriorityID': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'RecordedLength': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'SoundFileID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'SoundFileID': "{'title_ui': 'Datum/Tid/Skapad', 'description': 'Ljudfil skapad (tidsstämpel öppnad)'}", 'CreatedByUserID': "{'title_ui': 'Skapad av/Dikterad av', 'description': 'Ljudfil skapad av'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'CreatedByUser': "{'title_ui': 'Skapad av/Dikterad av', 'description': 'Ljudfil skapad av'}", 'SavedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Sparad på/Tillhör vårdenhet'}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på/Vårdenhet', 'description': 'Vårdenhets ID, original (dokumentet skapat på denna arb.plats). Ändras inte efter att dokumentet skapats.'}", 'RegistrationStatusID': "{'title_ui': None, 'description': {'break': None}}", 'HistoryStatusID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Synlig i översikten över dikterade ljudfiler'}", 'SignerUserID': "{'title_ui': None, 'description': 'Signeringsansvarig'}", 'EventDate': "{'title_ui': 'Händelsedatum', 'description': 'Sätts automatiskt till datum då ny diktering öppnas, eller sätts därefter manuellt till förflutet datum (d.v.s. som måste vara innan användarens nutid).'}", 'EventTime': "{'title_ui': 'Händelsetid', 'description': 'Sätts automatiskt till tid då ny diktering öppnas, eller sätts därefter manuellt till förfluten tid (d.v.s. som måste vara innan användarens nutid).'}", 'LinkedDocumentTypeID': "{'title_ui': 'Dokumenttyp', 'description': {'break': [None, None]}}", 'LinkedDocumentTemplateID': "{'title_ui': 'Brevmall/Journalmall', 'description': 'Mall-id till använd (vårdenhetsspecifik) mall för kopplat dokument, d.v.s. för journalmall eller brevmall.'}", 'PriorityID': "{'title_ui': 'Prioritet', 'description': {'break': [None, None, None, None]}}", 'RecordedLength': '{\'title_ui\': None, \'description\': \'Längd på ljudfil i ms (millisekunder). Till följd av en bugg i gamla "Speech Mike Executive" kan detta värde saknas.\'}', 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Den yrkesgrupp användaren tillhör'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['CreatedByUserID', 'PatientID', 'SoundFileID']
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
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUser] AS VARCHAR(MAX)) AS [CreatedByUser],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CONVERT(varchar(max), [EventDate], 126) AS [EventDate],
		CONVERT(varchar(max), [EventTime], 126) AS [EventTime],
		CAST([HistoryStatusID] AS VARCHAR(MAX)) AS [HistoryStatusID],
		CAST([LinkedDocumentTemplateID] AS VARCHAR(MAX)) AS [LinkedDocumentTemplateID],
		CAST([LinkedDocumentTypeID] AS VARCHAR(MAX)) AS [LinkedDocumentTypeID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PriorityID] AS VARCHAR(MAX)) AS [PriorityID],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([RecordedLength] AS VARCHAR(MAX)) AS [RecordedLength],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedStatusID] AS VARCHAR(MAX)) AS [SavedStatusID],
		CAST([SignerUserID] AS VARCHAR(MAX)) AS [SignerUserID],
		CONVERT(varchar(max), [SoundFileID], 126) AS [SoundFileID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved] 
	FROM Intelligence.viewreader.vSoundFiles) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    