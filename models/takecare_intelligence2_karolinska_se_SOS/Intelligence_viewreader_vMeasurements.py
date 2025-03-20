
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Mätvärdestransaktioner. En transaktion kan ersättas av en nyare version eller makuleras, vilket visas med statuskoder. Mätvärden finns även i CaseNotes_Measurements och Emergency_TriageVitalMeasurements. Fr.o.m. version 13.4 fungerar versionshantering av mätvärden. Ett mätvärdes alla versioner har kolumnerna: PatientID, DocumentID, TimestampCreated, TermID och CreatedByUserID gemensamt. Versionerna åtskiljs av antingen kolumn TimestampSaved (servertidsstämpel) eller kolumn Version. Innan version 13.4 kan ev. kolumn TimestampSigned (klienttidsstämpel) ge en uppfattning om versionerna. Data för en journal innehåller samma DocumentID per patient upp till ca 1000 rader, då ett nytt DocumentID skapas.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AttesterUserID': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'IsGlobalTemplate': 'varchar(max)', 'MeasurementOrderDocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'Status': 'varchar(max)', 'TaskID': 'varchar(max)', 'TaskTermID': 'varchar(max)', 'TaskTimestamp': 'varchar(max)', 'TemplateID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TimestampSigned': 'varchar(max)', 'TriageMeasurementUUID': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Version': "{'title_ui': None, 'description': 'Versionsnummer tillagt i version 13.4'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': None}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När första versionen av transaktionen sparades'}", 'Status': "{'title_ui': None, 'description': {'break': [None, None]}}", 'SavedAtCareUnitID': "{'title_ui': 'Tillhör arbetsplats/Vårdenhet', 'description': 'Där data är registrerat'}", 'AttesterUserID': "{'title_ui': 'Vidimeringsansvarig', 'description': 'Den användare som är ansvarig för att vidimera'}", 'SignerUserID': "{'title_ui': None, 'description': 'Den användare som är ansvarig för att signera. I dagsläget signeras mätvärden direkt när de sparas.'}", 'SignedByUserID': "{'title_ui': None, 'description': 'Den användare som signerat'}", 'TimestampSigned': "{'title_ui': None, 'description': 'När transaktionen signerats'}", 'EventDate': "{'title_ui': 'Händelsetid', 'description': 'Det datum mätvärdet avser'}", 'EventTime': "{'title_ui': 'Händelsetid', 'description': 'Den tid mätvärdet avser'}", 'TermID': "{'title_ui': 'Term', 'description': 'Den term mätvärdet registrerats på'}", 'TemplateID': "{'title_ui': 'Mallnamn', 'description': 'Id för den mätvärdesmall som använts. Sätts inte när mätvärdet baseras på en beställning eller aktivitet.'}", 'IsGlobalTemplate': "{'title_ui': None, 'description': 'Om mätvärdesmallen är systemgemensam/global (annars per vårdenhet)'}", 'Value': "{'title_ui': 'Mätvärde', 'description': 'Registrerat mätvärde'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Fritextkommentar'}", 'TaskTimestamp': "{'title_ui': None, 'description': 'Tidsstämpel för kopplad aktivitet'}", 'TaskID': "{'title_ui': None, 'description': 'ID för kopplad aktivitet'}", 'TaskTermID': "{'title_ui': None, 'description': 'Term-id för kopplad aktivitet'}", 'MeasurementOrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för kopplad mätvärdesbeställning'}", 'TriageMeasurementUUID': "{'title_ui': None, 'description': 'UUID för kopplad triagering'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily"
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(AttesterUserID AS VARCHAR(MAX)) AS AttesterUserID,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CONVERT(varchar(max), EventDate, 126) AS EventDate,
		CONVERT(varchar(max), EventTime, 126) AS EventTime,
		CAST(IsGlobalTemplate AS VARCHAR(MAX)) AS IsGlobalTemplate,
		CAST(MeasurementOrderDocumentID AS VARCHAR(MAX)) AS MeasurementOrderDocumentID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CAST(Status AS VARCHAR(MAX)) AS Status,
		CAST(TaskID AS VARCHAR(MAX)) AS TaskID,
		CAST(TaskTermID AS VARCHAR(MAX)) AS TaskTermID,
		CONVERT(varchar(max), TaskTimestamp, 126) AS TaskTimestamp,
		CAST(TemplateID AS VARCHAR(MAX)) AS TemplateID,
		CAST(TermID AS VARCHAR(MAX)) AS TermID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CONVERT(varchar(max), TimestampSigned, 126) AS TimestampSigned,
		CAST(TriageMeasurementUUID AS VARCHAR(MAX)) AS TriageMeasurementUUID,
		CAST(Value AS VARCHAR(MAX)) AS Value,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMeasurements) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    