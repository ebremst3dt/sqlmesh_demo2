
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställning röntgen, Södertälje (Sectra).",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'History': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'IsPrelAnswer': 'varchar(max)', 'LabOrderSettingsID': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitExternalID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'OrdererComment': 'varchar(max)', 'PatientCalledFrom': 'varchar(max)', 'PatientCalledFromID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PlannedExaminationDate': 'varchar(max)', 'PlannedExaminationTime': 'varchar(max)', 'Questionnaire': 'varchar(max)', 'RID': 'varchar(max)', 'RecipientCareUnitExternalID': 'varchar(max)', 'RecipientCareUnitKombika': 'varchar(max)', 'RecipientSID': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralTime': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorTel': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TrackingDate': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'OrderedBy': "{'title_ui': 'Framställd av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd', 'description': None}", 'TrackingDate': "{'title_ui': None, 'description': 'Bevakningsdatum. Används inte än, nu lagras remissdatumet.'}", 'RID': "{'title_ui': 'RID:', 'description': None}", 'ReferralDate': "{'title_ui': 'Remissdatum', 'description': None}", 'ReferralTime': "{'title_ui': 'Remissdatum', 'description': None}", 'PlannedExaminationDate': "{'title_ui': 'Önskat datum', 'description': 'Visningen av fältet är inställningsbart'}", 'PlannedExaminationTime': "{'title_ui': 'Önskat datum', 'description': 'Visningen av fältet är inställningsbart'}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare/vidimeringsansvarig'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'ReplyRecipientCareUnitID': "{'title_ui': None, 'description': 'Svarsmottagare vårdenhet (Är ofta samma som beställande vårdenhet.)'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'RecipientCareUnitKombika': "{'title_ui': 'Mottagande lab', 'description': 'Till kombikakod/EXID. Visningen av fältet är inställningsbart'}", 'RecipientSID': "{'title_ui': None, 'description': 'Mottagarens SID.'}", 'Questionnaire': "{'title_ui': 'Frågeställning', 'description': None}", 'History': "{'title_ui': 'Anamnes och status', 'description': None}", 'IsPrelAnswer': "{'title_ui': 'Preliminärsvar', 'description': None}", 'ReferringDoctorTel': "{'title_ui': 'Telefon remittent', 'description': None}", 'PatientCalledFromID': "{'title_ui': 'Pat kallas från', 'description': 'Från generella register --> Labinställningar'}", 'PatientCalledFrom': "{'title_ui': 'Pat kallas från', 'description': 'Från generella register --> Labinställningar'}", 'IsEmergency': "{'title_ui': 'Akut', 'description': None}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'OrdererComment': "{'title_ui': 'Remisskommentar', 'description': None}", 'LabOrderSettingsID': "{'title_ui': None, 'description': 'ID i generella register --> Labinställningar'}", 'ExternalUnitIdTypeCode': "{'title_ui': 'Id-typ', 'description': 'Id-typ för beställningen'}", 'OrdererCareUnitExternalID': "{'title_ui': 'B:', 'description': 'Kod för extern enhet för beställare'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'S:', 'description': 'Kod för extern enhet för svarsmottagare'}", 'InvoiceeCareUnitExternalID': "{'title_ui': 'F:', 'description': 'Kod för extern enhet för fakturamottagare'}", 'RecipientCareUnitExternalID': "{'title_ui': 'Lab-kod', 'description': 'Kod för extern enhet för labb'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([ExternalUnitIdTypeCode] AS VARCHAR(MAX)) AS [ExternalUnitIdTypeCode],
		CAST([History] AS VARCHAR(MAX)) AS [History],
		CAST([InvoiceeCareUnitExternalID] AS VARCHAR(MAX)) AS [InvoiceeCareUnitExternalID],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsBloodInfection] AS VARCHAR(MAX)) AS [IsBloodInfection],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([IsPrelAnswer] AS VARCHAR(MAX)) AS [IsPrelAnswer],
		CAST([LabOrderSettingsID] AS VARCHAR(MAX)) AS [LabOrderSettingsID],
		CONVERT(varchar(max), [OrderDateTime], 126) AS [OrderDateTime],
		CAST([OrderedBy] AS VARCHAR(MAX)) AS [OrderedBy],
		CAST([OrdererCareUnitExternalID] AS VARCHAR(MAX)) AS [OrdererCareUnitExternalID],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([OrdererComment] AS VARCHAR(MAX)) AS [OrdererComment],
		CAST([PatientCalledFrom] AS VARCHAR(MAX)) AS [PatientCalledFrom],
		CAST([PatientCalledFromID] AS VARCHAR(MAX)) AS [PatientCalledFromID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PlannedExaminationDate], 126) AS [PlannedExaminationDate],
		CONVERT(varchar(max), [PlannedExaminationTime], 126) AS [PlannedExaminationTime],
		CAST([Questionnaire] AS VARCHAR(MAX)) AS [Questionnaire],
		CAST([RID] AS VARCHAR(MAX)) AS [RID],
		CAST([RecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [RecipientCareUnitExternalID],
		CAST([RecipientCareUnitKombika] AS VARCHAR(MAX)) AS [RecipientCareUnitKombika],
		CAST([RecipientSID] AS VARCHAR(MAX)) AS [RecipientSID],
		CONVERT(varchar(max), [ReferralDate], 126) AS [ReferralDate],
		CONVERT(varchar(max), [ReferralTime], 126) AS [ReferralTime],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReferringDoctorTel] AS VARCHAR(MAX)) AS [ReferringDoctorTel],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringDoctorUserName] AS VARCHAR(MAX)) AS [ReferringDoctorUserName],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([ReplyRecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitExternalID],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SavedStatusID] AS VARCHAR(MAX)) AS [SavedStatusID],
		CAST([SignedBy] AS VARCHAR(MAX)) AS [SignedBy],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [TrackingDate], 126) AS [TrackingDate],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vRadiologyOrdersSectra) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    