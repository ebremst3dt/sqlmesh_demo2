
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Webcert, status för hantering av händelser vid skapande av läkarintyg",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'LastReplyDatetime': 'varchar(max)', 'PatientID': 'varchar(max)', 'QuestionSubjectCode': 'varchar(max)', 'QuestionSubjectName': 'varchar(max)', 'ReceivedQuestionsAcknowledged': 'varchar(max)', 'ReceivedQuestionsAnswered': 'varchar(max)', 'ReceivedQuestionsNonAnswered': 'varchar(max)', 'ReceivedQuestionsTotal': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SentQuestionsAcknowledged': 'varchar(max)', 'SentQuestionsAnswered': 'varchar(max)', 'SentQuestionsNonAnswered': 'varchar(max)', 'SentQuestionsTotal': 'varchar(max)', 'SignedByUser': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignerUser': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'StatusID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TimestampSigned': 'varchar(max)', 'Version': 'varchar(max)', 'WebcertCertificateTypeCode': 'varchar(max)', 'WebcertCertificateTypeName': 'varchar(max)', 'WebcertCode': 'varchar(max)', 'WebcertEventCode': 'varchar(max)', 'WebcertSavedDatetime': 'varchar(max)', 'WebcertSentDatetime': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': 'Pnr', 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument'}", 'StatusID': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None]}}", 'TimestampSaved': "{'title_ui': 'Datum/tid', 'description': 'Version skapad'}", 'SavedByUserID': "{'title_ui': 'Ansvarig', 'description': 'Version sparad'}", 'TimestampCreated': "{'title_ui': None, 'description': 'Dokument skapat'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'Skapad av'}", 'SavedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Version skapad på'}", 'CreatedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Den vårdenhet där webcertifikatet är skapat'}", 'WebcertCode': "{'title_ui': None, 'description': 'Unikt id för intyg, satt av webcert'}", 'WebcertSavedDatetime': "{'title_ui': None, 'description': 'Senast sparad i Webcert'}", 'WebcertEventCode': "{'title_ui': None, 'description': 'Händelsekod, från socialstyrelsen'}", 'WebcertSentDatetime': "{'title_ui': None, 'description': 'Tid skickad från webcert'}", 'WebcertCertificateTypeCode': "{'title_ui': None, 'description': 'Intygstypskod, se kodtabell'}", 'WebcertCertificateTypeName': "{'title_ui': None, 'description': 'Namn på intygstyp som fanns i kodtabell vid skapandet av webcert, se kolumn WebcertCertificateTypeCode'}", 'TimestampSigned': "{'title_ui': None, 'description': 'Signerad'}", 'SignerUserID': "{'title_ui': None, 'description': 'Signeringsansvarig'}", 'SignerUser': "{'title_ui': None, 'description': 'Signeringsansvarig'}", 'SignedByUserID': "{'title_ui': None, 'description': 'Signerad av'}", 'SignedByUser': "{'title_ui': None, 'description': 'Signerad av'}", 'QuestionSubjectCode': "{'title_ui': None, 'description': 'Frågeämnekod'}", 'QuestionSubjectName': "{'title_ui': None, 'description': 'Frågeämnenamn'}", 'LastReplyDatetime': "{'title_ui': 'Svara senast', 'description': 'Datum då intygsmottagaren senast vill ha ett svar på en fråga'}", 'SentQuestionsTotal': "{'title_ui': None, 'description': 'Frågor totalt från läkare'}", 'SentQuestionsNonAnswered': "{'title_ui': None, 'description': 'Frågor ej besvarade'}", 'SentQuestionsAnswered': "{'title_ui': None, 'description': 'Frågor besvarade'}", 'SentQuestionsAcknowledged': "{'title_ui': None, 'description': 'Frågor hanterade'}", 'ReceivedQuestionsTotal': "{'title_ui': None, 'description': 'Svar totalt'}", 'ReceivedQuestionsNonAnswered': "{'title_ui': None, 'description': 'Svar ej besvarade'}", 'ReceivedQuestionsAnswered': "{'title_ui': None, 'description': 'Svar besvarade'}", 'ReceivedQuestionsAcknowledged': "{'title_ui': None, 'description': 'Svar hanterade'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CONVERT(varchar(max), LastReplyDatetime, 126) AS LastReplyDatetime,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(QuestionSubjectCode AS VARCHAR(MAX)) AS QuestionSubjectCode,
		CAST(QuestionSubjectName AS VARCHAR(MAX)) AS QuestionSubjectName,
		CAST(ReceivedQuestionsAcknowledged AS VARCHAR(MAX)) AS ReceivedQuestionsAcknowledged,
		CAST(ReceivedQuestionsAnswered AS VARCHAR(MAX)) AS ReceivedQuestionsAnswered,
		CAST(ReceivedQuestionsNonAnswered AS VARCHAR(MAX)) AS ReceivedQuestionsNonAnswered,
		CAST(ReceivedQuestionsTotal AS VARCHAR(MAX)) AS ReceivedQuestionsTotal,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SentQuestionsAcknowledged AS VARCHAR(MAX)) AS SentQuestionsAcknowledged,
		CAST(SentQuestionsAnswered AS VARCHAR(MAX)) AS SentQuestionsAnswered,
		CAST(SentQuestionsNonAnswered AS VARCHAR(MAX)) AS SentQuestionsNonAnswered,
		CAST(SentQuestionsTotal AS VARCHAR(MAX)) AS SentQuestionsTotal,
		CAST(SignedByUser AS VARCHAR(MAX)) AS SignedByUser,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CAST(SignerUser AS VARCHAR(MAX)) AS SignerUser,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CAST(StatusID AS VARCHAR(MAX)) AS StatusID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CONVERT(varchar(max), TimestampSigned, 126) AS TimestampSigned,
		CAST(Version AS VARCHAR(MAX)) AS Version,
		CAST(WebcertCertificateTypeCode AS VARCHAR(MAX)) AS WebcertCertificateTypeCode,
		CAST(WebcertCertificateTypeName AS VARCHAR(MAX)) AS WebcertCertificateTypeName,
		CAST(WebcertCode AS VARCHAR(MAX)) AS WebcertCode,
		CAST(WebcertEventCode AS VARCHAR(MAX)) AS WebcertEventCode,
		CONVERT(varchar(max), WebcertSavedDatetime, 126) AS WebcertSavedDatetime,
		CONVERT(varchar(max), WebcertSentDatetime, 126) AS WebcertSentDatetime 
	FROM Intelligence.viewreader.vWebcertCertificates) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    