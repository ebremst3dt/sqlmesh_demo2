
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställning Fysiologikliniken (Karolinska Huddinge och Solna).",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExamLocation': 'varchar(max)', 'ExamLocationCode': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'GroupRISCode': 'varchar(max)', 'History': 'varchar(max)', 'InterpreterLanguage': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsInterpreterNeeded': 'varchar(max)', 'IsPreliminaryAnswerRequested': 'varchar(max)', 'LabOrderSettingsID': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitExternalID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'OtherInfo': 'varchar(max)', 'PatientCalledFrom': 'varchar(max)', 'PatientCalledFromCode': 'varchar(max)', 'PatientCalledFromID': 'varchar(max)', 'PatientHeight': 'varchar(max)', 'PatientID': 'varchar(max)', 'PatientWeight': 'varchar(max)', 'PreliminaryAnswerTelNo': 'varchar(max)', 'Priority': 'varchar(max)', 'PriorityCode': 'varchar(max)', 'Questionnaire': 'varchar(max)', 'RID': 'varchar(max)', 'RecipientAddressRow1': 'varchar(max)', 'RecipientAddressRow2': 'varchar(max)', 'RecipientAddressRow3': 'varchar(max)', 'RecipientAddressRow4': 'varchar(max)', 'RecipientAddressRow5': 'varchar(max)', 'RecipientCareUnitExternalID': 'varchar(max)', 'RecipientCareUnitKombika': 'varchar(max)', 'RecipientSID': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralTime': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TrackingDate': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'OrderedBy': "{'title_ui': 'Framställd av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd', 'description': None}", 'TrackingDate': "{'title_ui': None, 'description': 'Bevakningsdatum. Används inte än, nu lagras datumet då beställningen skapades.'}", 'RID': "{'title_ui': 'RID', 'description': None}", 'ReferralDate': "{'title_ui': 'Remissdatum, tid', 'description': None}", 'ReferralTime': "{'title_ui': 'Remissdatum, tid', 'description': None}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Svarsmottagare vårdenhet (Är ofta samma som beställande vårdenhet.)'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'RecipientCareUnitKombika': "{'title_ui': None, 'description': 'Till kombikakod/EXID'}", 'RecipientSID': "{'title_ui': None, 'description': 'Mottagarens SID.'}", 'RecipientAddressRow1': "{'title_ui': 'Till', 'description': 'Mottagarens adress 1. Vid pappersremiss från fältet Adress, i övrigt från vårdenhetsregistret.'}", 'RecipientAddressRow2': "{'title_ui': 'Till', 'description': 'Mottagarens adress 2'}", 'RecipientAddressRow3': "{'title_ui': 'Till', 'description': 'Mottagarens adress 3'}", 'RecipientAddressRow4': "{'title_ui': 'Till', 'description': 'Mottagarens adress 4'}", 'RecipientAddressRow5': "{'title_ui': 'Till', 'description': 'Mottagarens adress 5'}", 'PatientHeight': "{'title_ui': 'Längd', 'description': None}", 'PatientWeight': "{'title_ui': 'Vikt', 'description': None}", 'GroupRISCode': "{'title_ui': 'Grupperingar', 'description': 'Kod för grupperingar, endast solna'}", 'Questionnaire': "{'title_ui': 'Frågeställning', 'description': None}", 'History': "{'title_ui': 'Anamnes, status', 'description': None}", 'OtherInfo': "{'title_ui': 'Övrigt', 'description': None}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'PriorityCode': "{'title_ui': 'Prioritet', 'description': {'break': None}}", 'Priority': "{'title_ui': 'Prioritet', 'description': {'break': None}}", 'IsPreliminaryAnswerRequested': "{'title_ui': 'Preliminärsvar', 'description': None}", 'PreliminaryAnswerTelNo': "{'title_ui': 'Tel/Sökare', 'description': None}", 'PatientCalledFromID': "{'title_ui': 'Pat kallas från', 'description': {'break': None}}", 'PatientCalledFrom': "{'title_ui': 'Pat kallas från', 'description': None}", 'PatientCalledFromCode': "{'title_ui': 'Pat kallas från', 'description': 'RIS-Kod, lagras endast om Codes_KodakCalledFrom används.'}", 'IsInterpreterNeeded': "{'title_ui': 'Tolk önskas', 'description': None}", 'InterpreterLanguage': "{'title_ui': 'Tolk önskas', 'description': 'Angivet språk'}", 'ExamLocationCode': "{'title_ui': 'Bör undersökas på', 'description': {'break': [None, None, None]}}", 'ExamLocation': "{'title_ui': 'Bör undersökas på', 'description': 'Var undersökningen bör utföras'}", 'LabOrderSettingsID': "{'title_ui': None, 'description': 'ID i generella register --> Labinställningar. Endast Solna'}", 'ExternalUnitIdTypeCode': "{'title_ui': 'Id-typ', 'description': 'Id-typ för beställningen'}", 'OrdererCareUnitExternalID': "{'title_ui': 'B:', 'description': 'Kod för extern enhet för beställare'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'S:', 'description': 'Kod för extern enhet för svarsmottagare'}", 'InvoiceeCareUnitExternalID': "{'title_ui': 'F:', 'description': 'Kod för extern enhet för fakturamottagare'}", 'RecipientCareUnitExternalID': "{'title_ui': 'Lab-kod', 'description': 'Kod för extern enhet för labb'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(ExamLocation AS VARCHAR(MAX)) AS ExamLocation,
		CAST(ExamLocationCode AS VARCHAR(MAX)) AS ExamLocationCode,
		CAST(ExternalUnitIdTypeCode AS VARCHAR(MAX)) AS ExternalUnitIdTypeCode,
		CAST(GroupRISCode AS VARCHAR(MAX)) AS GroupRISCode,
		CAST(History AS VARCHAR(MAX)) AS History,
		CAST(InterpreterLanguage AS VARCHAR(MAX)) AS InterpreterLanguage,
		CAST(InvoiceeCareUnitExternalID AS VARCHAR(MAX)) AS InvoiceeCareUnitExternalID,
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(IsBloodInfection AS VARCHAR(MAX)) AS IsBloodInfection,
		CAST(IsInterpreterNeeded AS VARCHAR(MAX)) AS IsInterpreterNeeded,
		CAST(IsPreliminaryAnswerRequested AS VARCHAR(MAX)) AS IsPreliminaryAnswerRequested,
		CAST(LabOrderSettingsID AS VARCHAR(MAX)) AS LabOrderSettingsID,
		CONVERT(varchar(max), OrderDateTime, 126) AS OrderDateTime,
		CAST(OrderedBy AS VARCHAR(MAX)) AS OrderedBy,
		CAST(OrdererCareUnitExternalID AS VARCHAR(MAX)) AS OrdererCareUnitExternalID,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(OtherInfo AS VARCHAR(MAX)) AS OtherInfo,
		CAST(PatientCalledFrom AS VARCHAR(MAX)) AS PatientCalledFrom,
		CAST(PatientCalledFromCode AS VARCHAR(MAX)) AS PatientCalledFromCode,
		CAST(PatientCalledFromID AS VARCHAR(MAX)) AS PatientCalledFromID,
		CAST(PatientHeight AS VARCHAR(MAX)) AS PatientHeight,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PatientWeight AS VARCHAR(MAX)) AS PatientWeight,
		CAST(PreliminaryAnswerTelNo AS VARCHAR(MAX)) AS PreliminaryAnswerTelNo,
		CAST(Priority AS VARCHAR(MAX)) AS Priority,
		CAST(PriorityCode AS VARCHAR(MAX)) AS PriorityCode,
		CAST(Questionnaire AS VARCHAR(MAX)) AS Questionnaire,
		CAST(RID AS VARCHAR(MAX)) AS RID,
		CAST(RecipientAddressRow1 AS VARCHAR(MAX)) AS RecipientAddressRow1,
		CAST(RecipientAddressRow2 AS VARCHAR(MAX)) AS RecipientAddressRow2,
		CAST(RecipientAddressRow3 AS VARCHAR(MAX)) AS RecipientAddressRow3,
		CAST(RecipientAddressRow4 AS VARCHAR(MAX)) AS RecipientAddressRow4,
		CAST(RecipientAddressRow5 AS VARCHAR(MAX)) AS RecipientAddressRow5,
		CAST(RecipientCareUnitExternalID AS VARCHAR(MAX)) AS RecipientCareUnitExternalID,
		CAST(RecipientCareUnitKombika AS VARCHAR(MAX)) AS RecipientCareUnitKombika,
		CAST(RecipientSID AS VARCHAR(MAX)) AS RecipientSID,
		CONVERT(varchar(max), ReferralDate, 126) AS ReferralDate,
		CONVERT(varchar(max), ReferralTime, 126) AS ReferralTime,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReferringDoctorUserID AS VARCHAR(MAX)) AS ReferringDoctorUserID,
		CAST(ReferringDoctorUserName AS VARCHAR(MAX)) AS ReferringDoctorUserName,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(ReplyRecipientCareUnitExternalID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitExternalID,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SavedStatusID AS VARCHAR(MAX)) AS SavedStatusID,
		CAST(SignedBy AS VARCHAR(MAX)) AS SignedBy,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CONVERT(varchar(max), SignedDatetime, 126) AS SignedDatetime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CONVERT(varchar(max), TrackingDate, 126) AS TrackingDate,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPhysiologyOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    