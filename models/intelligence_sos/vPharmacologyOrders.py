
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Farmakologibeställningar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'GUID': 'varchar(max)', 'HistoryQuestionnaire': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'LID': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientCreatinine': 'varchar(max)', 'PatientCreatinineDate': 'varchar(max)', 'PatientHeight': 'varchar(max)', 'PatientID': 'varchar(max)', 'PatientIsSmokerCode': 'varchar(max)', 'PatientIsSmokerText': 'varchar(max)', 'PatientWeight': 'varchar(max)', 'PlannedSamplingDate': 'varchar(max)', 'PlannedSamplingTime': 'varchar(max)', 'RID': 'varchar(max)', 'RecipientCareUnitKombika': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplingDatetime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SerialOrderNumber': 'varchar(max)', 'SerialOrderRID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'TestCauseCode': 'varchar(max)', 'TestCauseText': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TreatmentCauseCode': 'varchar(max)', 'TreatmentCauseText': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'OrderedBy': "{'title_ui': 'Framställd av/Sparad av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd/Sparad', 'description': None}", 'RID': "{'title_ui': 'R:', 'description': None}", 'LID': "{'title_ui': 'L:', 'description': 'Om NULL lagras, visas 000-000-0 i TC'}", 'GUID': "{'title_ui': None, 'description': 'Unikt ID på beställningen'}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig.'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Svarsmottagare vårdenhet (Är ofta samma som beställande vårdenhet.)'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'RecipientCareUnitKombika': "{'title_ui': None, 'description': 'Till kombikakod/EXID'}", 'SerialOrderNumber': "{'title_ui': 'Provtagning nr', 'description': None}", 'SerialOrderRID': "{'title_ui': 'i serien', 'description': None}", 'SamplingDatetime': "{'title_ui': 'Provtagningstid', 'description': 'Provtagningstidpunkt'}", 'PlannedSamplingDate': "{'title_ui': 'Planerad prov. tid', 'description': None}", 'PlannedSamplingTime': "{'title_ui': 'Planerad prov. tid', 'description': None}", 'IsEmergency': "{'title_ui': 'Prioritet', 'description': None}", 'TreatmentCauseCode': "{'title_ui': 'Orsak till behandling', 'description': 'Orsak till behandling, kod'}", 'TreatmentCauseText': "{'title_ui': 'Orsak till behandling', 'description': 'Orsak till behandling, text'}", 'TestCauseCode': "{'title_ui': 'Orsak till provtagning', 'description': 'Orsak till provtagning, kod'}", 'TestCauseText': "{'title_ui': 'Orsak till provtagning', 'description': 'Orsak till provtagning, text'}", 'HistoryQuestionnaire': "{'title_ui': 'Upplysn./frågeställning', 'description': None}", 'PatientHeight': "{'title_ui': 'Längd', 'description': None}", 'PatientWeight': "{'title_ui': 'Vikt', 'description': None}", 'PatientCreatinine': "{'title_ui': 'P-Kreatinin', 'description': None}", 'PatientCreatinineDate': "{'title_ui': 'P-Kreatinin', 'description': None}", 'PatientIsSmokerCode': "{'title_ui': 'Rökare', 'description': None}", 'PatientIsSmokerText': "{'title_ui': 'Rökare', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID', 'Version']
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
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([GUID] AS VARCHAR(MAX)) AS [GUID],
		CAST([HistoryQuestionnaire] AS VARCHAR(MAX)) AS [HistoryQuestionnaire],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CONVERT(varchar(max), [OrderDateTime], 126) AS [OrderDateTime],
		CAST([OrderedBy] AS VARCHAR(MAX)) AS [OrderedBy],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([PatientCreatinine] AS VARCHAR(MAX)) AS [PatientCreatinine],
		CONVERT(varchar(max), [PatientCreatinineDate], 126) AS [PatientCreatinineDate],
		CAST([PatientHeight] AS VARCHAR(MAX)) AS [PatientHeight],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PatientIsSmokerCode] AS VARCHAR(MAX)) AS [PatientIsSmokerCode],
		CAST([PatientIsSmokerText] AS VARCHAR(MAX)) AS [PatientIsSmokerText],
		CAST([PatientWeight] AS VARCHAR(MAX)) AS [PatientWeight],
		CONVERT(varchar(max), [PlannedSamplingDate], 126) AS [PlannedSamplingDate],
		CONVERT(varchar(max), [PlannedSamplingTime], 126) AS [PlannedSamplingTime],
		CAST([RID] AS VARCHAR(MAX)) AS [RID],
		CAST([RecipientCareUnitKombika] AS VARCHAR(MAX)) AS [RecipientCareUnitKombika],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringDoctorUserName] AS VARCHAR(MAX)) AS [ReferringDoctorUserName],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CONVERT(varchar(max), [SamplingDatetime], 126) AS [SamplingDatetime],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SavedStatusID] AS VARCHAR(MAX)) AS [SavedStatusID],
		CAST([SerialOrderNumber] AS VARCHAR(MAX)) AS [SerialOrderNumber],
		CAST([SerialOrderRID] AS VARCHAR(MAX)) AS [SerialOrderRID],
		CAST([SignedBy] AS VARCHAR(MAX)) AS [SignedBy],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CAST([TestCauseCode] AS VARCHAR(MAX)) AS [TestCauseCode],
		CAST([TestCauseText] AS VARCHAR(MAX)) AS [TestCauseText],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([TreatmentCauseCode] AS VARCHAR(MAX)) AS [TreatmentCauseCode],
		CAST([TreatmentCauseText] AS VARCHAR(MAX)) AS [TreatmentCauseText],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vPharmacologyOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    