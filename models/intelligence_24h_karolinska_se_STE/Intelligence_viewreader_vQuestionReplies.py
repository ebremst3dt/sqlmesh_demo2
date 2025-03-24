
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="I denna tabell lagras versioner av Fråga/svars-dokument.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ComplementDocumentID': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExternalDocumentID': 'varchar(max)', 'ExternalSystemID': 'varchar(max)', 'IsCancellationQuestion': 'varchar(max)', 'LatestReplyDate': 'varchar(max)', 'LinkedDocumentSubTypeID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReplySavedAtCareUnitID': 'varchar(max)', 'ReplySavedByUserID': 'varchar(max)', 'ReplyTimestampSaved': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUser': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignerUser': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'StatusID': 'varchar(max)', 'SubjectCode': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TimestampSigned': 'varchar(max)', 'UUID': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': 'Pnr', 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'StatusID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'TimestampSaved': "{'title_ui': 'Datum/tid', 'description': 'Version skapad'}", 'SavedByUserID': "{'title_ui': 'Ansvarig', 'description': 'Version skapad av'}", 'SavedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Version skapad på'}", 'TimestampCreated': "{'title_ui': None, 'description': 'Skapad'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'Skapad av'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat'}", 'ReplyTimestampSaved': "{'title_ui': None, 'description': 'Svar sparat'}", 'ReplySavedByUserID': "{'title_ui': None, 'description': 'Svar sparat av'}", 'ReplySavedAtCareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet där svaret är sparat'}", 'TimestampSigned': "{'title_ui': None, 'description': 'Signerad'}", 'SignedByUserID': "{'title_ui': None, 'description': 'Signerad av'}", 'SignedByUser': "{'title_ui': None, 'description': 'Signerad av'}", 'SignerUserID': "{'title_ui': None, 'description': 'Signeringsansvarig'}", 'SignerUser': "{'title_ui': None, 'description': 'Signeringsansvarig'}", 'UUID': "{'title_ui': None, 'description': 'Internt UUID på dokumentet'}", 'ExternalSystemID': "{'title_ui': None, 'description': {'break': None}}", 'ExternalDocumentID': "{'title_ui': None, 'description': 'Externa systemets id på dokumentet (gemensamt för alla versioner)'}", 'SubjectCode': "{'title_ui': 'Ämne', 'description': {'break': [None, None, None, None, None, None, None]}}", 'LinkedDocumentSubTypeID': "{'title_ui': None, 'description': 'Subtyp för kopplat dokument. Exempelvis mall-id för Formulär (dokumenttyp 54).'}", 'ComplementDocumentID': "{'title_ui': None, 'description': 'Dokument-id för eventuell komplettering, t.ex. kompletterande Formulär. Endast aktuell om StatusID=3.'}", 'LatestReplyDate': "{'title_ui': 'Sista datum för svar/Svara senast', 'description': 'Svara senast'}", 'IsCancellationQuestion': "{'title_ui': None, 'description': 'Är makuleringsfråga'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ComplementDocumentID] AS VARCHAR(MAX)) AS [ComplementDocumentID],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([ExternalDocumentID] AS VARCHAR(MAX)) AS [ExternalDocumentID],
		CAST([ExternalSystemID] AS VARCHAR(MAX)) AS [ExternalSystemID],
		CAST([IsCancellationQuestion] AS VARCHAR(MAX)) AS [IsCancellationQuestion],
		CONVERT(varchar(max), [LatestReplyDate], 126) AS [LatestReplyDate],
		CAST([LinkedDocumentSubTypeID] AS VARCHAR(MAX)) AS [LinkedDocumentSubTypeID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ReplySavedAtCareUnitID] AS VARCHAR(MAX)) AS [ReplySavedAtCareUnitID],
		CAST([ReplySavedByUserID] AS VARCHAR(MAX)) AS [ReplySavedByUserID],
		CONVERT(varchar(max), [ReplyTimestampSaved], 126) AS [ReplyTimestampSaved],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SignedByUser] AS VARCHAR(MAX)) AS [SignedByUser],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CAST([SignerUser] AS VARCHAR(MAX)) AS [SignerUser],
		CAST([SignerUserID] AS VARCHAR(MAX)) AS [SignerUserID],
		CAST([StatusID] AS VARCHAR(MAX)) AS [StatusID],
		CAST([SubjectCode] AS VARCHAR(MAX)) AS [SubjectCode],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [TimestampSigned], 126) AS [TimestampSigned],
		CAST([UUID] AS VARCHAR(MAX)) AS [UUID],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vQuestionReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    