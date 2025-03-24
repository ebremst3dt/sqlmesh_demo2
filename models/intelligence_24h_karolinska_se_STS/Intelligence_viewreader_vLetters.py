
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Brev/Kallelser - Brev och vissa kallelser sparas här. Endast de kallelser som användaren öppnat och valt att spara, kommer att sparas här.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BodyFontSize': 'varchar(max)', 'BodyPage1': 'varchar(max)', 'BodyPage2': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'CreatedFromDocument': 'varchar(max)', 'CreatedFromDocumentTypeID': 'varchar(max)', 'CreatedFromLetterTemplateID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DocumentName': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'FooterLine1': 'varchar(max)', 'FooterLine2': 'varchar(max)', 'FooterLine3': 'varchar(max)', 'FooterLine4': 'varchar(max)', 'Heading': 'varchar(max)', 'HeadingFontSize': 'varchar(max)', 'LetterTypeID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Recipient': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedByUser': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'Sender': 'varchar(max)', 'SignedByUser': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUser': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatusID': "{'title_ui': None, 'description': {'break': None}}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedByUser': "{'title_ui': 'Version skapad av', 'description': 'Namn på den som skriver.'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'Original skapat av'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Vårdenhets ID, original (dokumentet skapat på denna arb.plats). Styr behörigheter. Ändras inte efter att dokumentet skapats.'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Syns i brevöversikten. Består av brevrubrik och mottagare, ev. föregångna av dokumentnamn och/eller <MAKUKLERAD>'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': None}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': None}", 'SignedByUser': "{'title_ui': 'Signerad av', 'description': None}", 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': None}", 'SignerUser': "{'title_ui': 'Signeringsansvarig', 'description': None}", 'CreatedFromDocumentTypeID': "{'title_ui': None, 'description': 'Om brevet/kallelsen skapats från ett annat dokument så lagras dokumenttyps-id för det dokumentet här'}", 'CreatedFromDocument': "{'title_ui': None, 'description': 'Om brevet/kallelsen skapats från ett annat dokument så lagras detta dokuments id (nyckel) här'}", 'CreatedFromLetterTemplateID': "{'title_ui': None, 'description': 'För kallelser som skapats från annat dokument, eller för brev som skapats från och med version 12.1, lagras alltid brevmallens (vårdenhetsspecifika) id här'}", 'LetterTypeID': "{'title_ui': None, 'description': {'break': None}}", 'EventDate': "{'title_ui': 'Händelsetid', 'description': None}", 'EventTime': "{'title_ui': 'Händelsetid', 'description': None}", 'Sender': "{'title_ui': 'Avsändare', 'description': 'Internt lagras en textvektor per avsändarrad i brevet. Raderna är här ihopslagna och åtskilda med <nl>.'}", 'Recipient': "{'title_ui': 'Mottagare', 'description': 'Internt lagras en textvektor per mottagarrad i brevet. Raderna är här ihopslagna och åtskilda med <nl>.'}", 'DocumentName': "{'title_ui': 'Dokumentnamn', 'description': 'Kan sättas av användaren. Är även synlig som del av kommentar i översikten över brev.'}", 'Heading': "{'title_ui': 'Rubrik', 'description': None}", 'BodyPage1': "{'title_ui': None, 'description': 'Översätts från rtf-format och lagras med interna formattaggar liksom i journaltext.'}", 'BodyPage2': "{'title_ui': None, 'description': 'Används ej men gammalt data kan finnas.'}", 'FooterLine1': "{'title_ui': None, 'description': 'Sidfot, rad 1'}", 'FooterLine2': "{'title_ui': None, 'description': 'Sidfot, rad 2'}", 'FooterLine3': "{'title_ui': None, 'description': 'Sidfot, rad 3'}", 'FooterLine4': "{'title_ui': None, 'description': 'Sidfot, rad 4'}", 'BodyFontSize': "{'title_ui': None, 'description': 'Fontstorlek brödtext'}", 'HeadingFontSize': "{'title_ui': None, 'description': 'Fontstorlek rubrik'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BodyFontSize] AS VARCHAR(MAX)) AS [BodyFontSize],
		CAST([BodyPage1] AS VARCHAR(MAX)) AS [BodyPage1],
		CAST([BodyPage2] AS VARCHAR(MAX)) AS [BodyPage2],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([CreatedFromDocument] AS VARCHAR(MAX)) AS [CreatedFromDocument],
		CAST([CreatedFromDocumentTypeID] AS VARCHAR(MAX)) AS [CreatedFromDocumentTypeID],
		CAST([CreatedFromLetterTemplateID] AS VARCHAR(MAX)) AS [CreatedFromLetterTemplateID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([DocumentName] AS VARCHAR(MAX)) AS [DocumentName],
		CONVERT(varchar(max), [EventDate], 126) AS [EventDate],
		CONVERT(varchar(max), [EventTime], 126) AS [EventTime],
		CAST([FooterLine1] AS VARCHAR(MAX)) AS [FooterLine1],
		CAST([FooterLine2] AS VARCHAR(MAX)) AS [FooterLine2],
		CAST([FooterLine3] AS VARCHAR(MAX)) AS [FooterLine3],
		CAST([FooterLine4] AS VARCHAR(MAX)) AS [FooterLine4],
		CAST([Heading] AS VARCHAR(MAX)) AS [Heading],
		CAST([HeadingFontSize] AS VARCHAR(MAX)) AS [HeadingFontSize],
		CAST([LetterTypeID] AS VARCHAR(MAX)) AS [LetterTypeID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Recipient] AS VARCHAR(MAX)) AS [Recipient],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([SavedByUser] AS VARCHAR(MAX)) AS [SavedByUser],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([Sender] AS VARCHAR(MAX)) AS [Sender],
		CAST([SignedByUser] AS VARCHAR(MAX)) AS [SignedByUser],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CAST([SignerUser] AS VARCHAR(MAX)) AS [SignerUser],
		CAST([SignerUserID] AS VARCHAR(MAX)) AS [SignerUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vLetters) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    