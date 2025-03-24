
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Aktiviteter används som en Att-göra lista för patienten. Aktiviteter kan vara av olika typ, kontinuerliga, engångs osv. Typspecifik information lagras i andra tabeller.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompletedDate': 'varchar(max)', 'CompletedTime': 'varchar(max)', 'CompletedTimestamp': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'ExplanationInstruction': 'varchar(max)', 'LinkedDocumentID': 'varchar(max)', 'LinkedDocumentTypeID': 'varchar(max)', 'LockedByUserID': 'varchar(max)', 'LockedTimestamp': 'varchar(max)', 'PatientID': 'varchar(max)', 'PriorityID': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'Status': 'varchar(max)', 'TaskID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När den första versionen av aktiviteten skapades'}", 'TaskID': "{'title_ui': None, 'description': 'Används när flera aktiviteter skapats på samma sekund.'}", 'TermID': "{'title_ui': None, 'description': 'Den term som definierar aktivitetens namn.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Vårdenhet som dokumentet senast sparades på.'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Användare som skapat dokumentet från början.'}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': 'Användaren som är ansvarig för att signera dokumentet'}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Den yrkesgrupp som planeras utföra aktiviteten.'}", 'Status': "{'title_ui': 'Utförd', 'description': {'break': [None, None]}}", 'LinkedDocumentTypeID': "{'title_ui': 'Länkat dokument', 'description': 'Dokumenttyp för det dokument som skapade aktiviteten.'}", 'LinkedDocumentID': "{'title_ui': None, 'description': 'Dokument-id för det dokument som skapade aktiviteten. Används för att koppla ihop andra dokument i journalen med aktiviteter.'}", 'ExplanationInstruction': "{'title_ui': 'Förklaring/anvisning', 'description': 'Text med närmare instruktioner om vad som ska göras.'}", 'CompletedDate': "{'title_ui': 'Utförd/Utförd, faktiskt tid', 'description': 'Datum som användaren angett då aktiviteten utfördes, sattes ut, makulerades eller markerades som Ej utförd.'}", 'CompletedTime': "{'title_ui': 'Utförd/Utförd, faktiskt tid', 'description': 'Klockslag som användaren angett då aktiviteten utfördes, sattes ut, makulerades eller markerades som Ej utförd.'}", 'CompletedTimestamp': "{'title_ui': None, 'description': 'Tid då användaren markerade aktiviteten som utförd eller utsatt. Sätts av systemet.'}", 'LockedByUserID': "{'title_ui': 'Ordination låst av', 'description': 'Om aktiviteten är låst av någon. Låsta aktiviteter har begränsade redigeringsmöjligheter.'}", 'LockedTimestamp': "{'title_ui': 'Ordination låst', 'description': 'Om aktiviteten är låst, när den låstes'}", 'PriorityID': "{'title_ui': 'Prioritet', 'description': 'Aktivitetens valda prioritet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), [CompletedDate], 126) AS [CompletedDate],
		CONVERT(varchar(max), [CompletedTime], 126) AS [CompletedTime],
		CONVERT(varchar(max), [CompletedTimestamp], 126) AS [CompletedTimestamp],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([ExplanationInstruction] AS VARCHAR(MAX)) AS [ExplanationInstruction],
		CAST([LinkedDocumentID] AS VARCHAR(MAX)) AS [LinkedDocumentID],
		CAST([LinkedDocumentTypeID] AS VARCHAR(MAX)) AS [LinkedDocumentTypeID],
		CAST([LockedByUserID] AS VARCHAR(MAX)) AS [LockedByUserID],
		CONVERT(varchar(max), [LockedTimestamp], 126) AS [LockedTimestamp],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PriorityID] AS VARCHAR(MAX)) AS [PriorityID],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CAST([SignerUserID] AS VARCHAR(MAX)) AS [SignerUserID],
		CAST([Status] AS VARCHAR(MAX)) AS [Status],
		CAST([TaskID] AS VARCHAR(MAX)) AS [TaskID],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved] 
	FROM Intelligence.viewreader.vTasks) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    