
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställning Kemlabb (FlexLab).",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererAddressRow1': 'varchar(max)', 'OrdererAddressRow2': 'varchar(max)', 'OrdererAddressRow3': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'OrdererComment': 'varchar(max)', 'OrdererFax': 'varchar(max)', 'OrdererPostalCodeCity': 'varchar(max)', 'OrdererTelephone': 'varchar(max)', 'PatientID': 'varchar(max)', 'PlannedSamplingDate': 'varchar(max)', 'PlannedSamplingTime': 'varchar(max)', 'RecipientAddressRow1': 'varchar(max)', 'RecipientAddressRow2': 'varchar(max)', 'RecipientAddressRow3': 'varchar(max)', 'RecipientCareUnitKombika': 'varchar(max)', 'RecipientFax': 'varchar(max)', 'RecipientPostalCodeCity': 'varchar(max)', 'RecipientTelephone': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplerComment': 'varchar(max)', 'SamplingDatetime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SentTimestamp': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TrackingDate': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'TrackingDate': "{'title_ui': None, 'description': 'Bevakningsdatum. Används inte än, nu lagras datumet för den planerade provtagningstiden eller den faktiska provtagningstiden.'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'RecipientCareUnitKombika': "{'title_ui': None, 'description': 'Till kombikakod/EXID'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'ReferralID': "{'title_ui': 'RID', 'description': None}", 'OrdererAddressRow1': "{'title_ui': 'Från', 'description': 'Avsändarens adress 1'}", 'OrdererAddressRow2': "{'title_ui': 'Från', 'description': 'Avsändarens adress 2'}", 'OrdererAddressRow3': "{'title_ui': 'Från', 'description': 'Avsändarens adress 3'}", 'OrdererPostalCodeCity': "{'title_ui': 'Från', 'description': 'Avsändarens postadress'}", 'OrdererTelephone': "{'title_ui': 'Från', 'description': 'Avsändarens telefonnr'}", 'OrdererFax': "{'title_ui': 'Från', 'description': 'Avsändarens faxnr'}", 'RecipientAddressRow1': "{'title_ui': 'Till', 'description': 'Mottagarens adress 1'}", 'RecipientAddressRow2': "{'title_ui': 'Till', 'description': 'Mottagarens adress 2'}", 'RecipientAddressRow3': "{'title_ui': 'Till', 'description': 'Mottagarens adress 3'}", 'RecipientPostalCodeCity': "{'title_ui': 'Till', 'description': 'Mottagarens postadress'}", 'RecipientTelephone': "{'title_ui': 'Till', 'description': 'Mottagarens telefonnr'}", 'RecipientFax': "{'title_ui': 'Till', 'description': 'Mottagarens faxnr'}", 'SamplingDatetime': "{'title_ui': 'Provtagn. tid', 'description': 'Provtagningstidpunkt'}", 'OrdererComment': "{'title_ui': 'Beställarens kommentarer/Kliniska upplysningar', 'description': None}", 'SamplerComment': "{'title_ui': 'Provtagarens kommentarer', 'description': None}", 'SentTimestamp': "{'title_ui': 'Beställd', 'description': 'Skickad'}", 'ReplyRecipientCareUnitID': "{'title_ui': None, 'description': 'Svarsmottagare vårdenhet'}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'OrderedBy': "{'title_ui': 'Framställd av', 'description': 'Framställd av användare'}", 'IsEmergency': "{'title_ui': 'Akut', 'description': None}", 'PlannedSamplingDate': "{'title_ui': 'Planerad prov.datum', 'description': None}", 'PlannedSamplingTime': "{'title_ui': 'Planerad prov.datum', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsBloodInfection] AS VARCHAR(MAX)) AS [IsBloodInfection],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([OrderedBy] AS VARCHAR(MAX)) AS [OrderedBy],
		CAST([OrdererAddressRow1] AS VARCHAR(MAX)) AS [OrdererAddressRow1],
		CAST([OrdererAddressRow2] AS VARCHAR(MAX)) AS [OrdererAddressRow2],
		CAST([OrdererAddressRow3] AS VARCHAR(MAX)) AS [OrdererAddressRow3],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([OrdererComment] AS VARCHAR(MAX)) AS [OrdererComment],
		CAST([OrdererFax] AS VARCHAR(MAX)) AS [OrdererFax],
		CAST([OrdererPostalCodeCity] AS VARCHAR(MAX)) AS [OrdererPostalCodeCity],
		CAST([OrdererTelephone] AS VARCHAR(MAX)) AS [OrdererTelephone],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PlannedSamplingDate], 126) AS [PlannedSamplingDate],
		CONVERT(varchar(max), [PlannedSamplingTime], 126) AS [PlannedSamplingTime],
		CAST([RecipientAddressRow1] AS VARCHAR(MAX)) AS [RecipientAddressRow1],
		CAST([RecipientAddressRow2] AS VARCHAR(MAX)) AS [RecipientAddressRow2],
		CAST([RecipientAddressRow3] AS VARCHAR(MAX)) AS [RecipientAddressRow3],
		CAST([RecipientCareUnitKombika] AS VARCHAR(MAX)) AS [RecipientCareUnitKombika],
		CAST([RecipientFax] AS VARCHAR(MAX)) AS [RecipientFax],
		CAST([RecipientPostalCodeCity] AS VARCHAR(MAX)) AS [RecipientPostalCodeCity],
		CAST([RecipientTelephone] AS VARCHAR(MAX)) AS [RecipientTelephone],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringDoctorUserName] AS VARCHAR(MAX)) AS [ReferringDoctorUserName],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([SamplerComment] AS VARCHAR(MAX)) AS [SamplerComment],
		CONVERT(varchar(max), [SamplingDatetime], 126) AS [SamplingDatetime],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SavedStatusID] AS VARCHAR(MAX)) AS [SavedStatusID],
		CONVERT(varchar(max), [SentTimestamp], 126) AS [SentTimestamp],
		CAST([SignedBy] AS VARCHAR(MAX)) AS [SignedBy],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [TrackingDate], 126) AS [TrackingDate],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vChemLabOrdersFlexLab) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    