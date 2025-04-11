
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Beställning Multidiciplinär.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CancellationComment': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'Disciplines': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'GUID': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsElectronic': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'IsWatched': 'varchar(max)', 'LabOrderSettingsID': 'varchar(max)', 'Material': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitExternalID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'OrdererComment': 'varchar(max)', 'PatientID': 'varchar(max)', 'PlannedSamplingDate': 'varchar(max)', 'PlannedSamplingTime': 'varchar(max)', 'RID': 'varchar(max)', 'RecipientCareUnitExternalID': 'varchar(max)', 'RecipientCareUnitKombika': 'varchar(max)', 'RecipientSID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplerComment': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingDatetime': 'varchar(max)', 'SamplingLocation': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)', 'WatchDate': 'varchar(max)', 'WatchStatusID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': 'Registreringsstatus-labbeställning i generella register kodtabeller'}", 'SavedStatusID': "{'title_ui': None, 'description': 'Lagringsstatus-labbeställning i generella register kodtabeller'}", 'WatchStatusID': "{'title_ui': None, 'description': 'Bevakningsstatus'}", 'IsWatched': "{'title_ui': 'Bevaka beställningen', 'description': 'En bevakning skapades när beställningen gjordes'}", 'WatchDate': "{'title_ui': 'Bevaka beställningen datum', 'description': 'Bevakningsdatum.'}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'Disciplines': "{'title_ui': 'Disciplin', 'description': 'Discipliner i beställningen'}", 'OrderedBy': "{'title_ui': 'Framställd av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd', 'description': None}", 'RID': "{'title_ui': 'RID:', 'description': None}", 'GUID': "{'title_ui': None, 'description': 'Unikt ID på beställningen'}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Svarsmottagare vårdenhet (Är ofta samma som beställande vårdenhet.)'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'RecipientCareUnitKombika': "{'title_ui': None, 'description': 'Till kombikakod/EXID'}", 'RecipientSID': "{'title_ui': None, 'description': 'Mottagarens SID.'}", 'IsElectronic': "{'title_ui': None, 'description': None}", 'LabOrderSettingsID': "{'title_ui': None, 'description': 'Identifierar ett labb och dess inställningar'}", 'SamplingDate': "{'title_ui': 'Provtagn. tid', 'description': 'Provtagningsdatum. Ny från version 15.1. Ersätter datum i kolumnen SamplingDatetime.'}", 'SamplingTime': "{'title_ui': 'Provtagn. tid', 'description': 'Provtagningstid. Ny från version 15.1. Ersätter tiden i kolumnen SamplingDatetime.'}", 'SamplingDatetime': "{'title_ui': 'Provtagn. tid', 'description': 'Provtagningstidpunkt. Ersätts från version 15.1 av de två kolumnerna SamplingDate och SamplingTime. Det kommer fortfarande att lagras en tidsstämpel i denna kolumn.'}", 'PlannedSamplingDate': "{'title_ui': 'Planerad tid', 'description': None}", 'PlannedSamplingTime': "{'title_ui': 'Planerad tid', 'description': None}", 'SamplingLocation': "{'title_ui': 'Provtagningsplats', 'description': {'break': None}}", 'OrdererComment': "{'title_ui': 'Remisskommentar', 'description': None}", 'SamplerComment': "{'title_ui': 'Provtagarens kommentar (lokal)', 'description': None}", 'CancellationComment': "{'title_ui': 'Makuleringskommentar', 'description': None}", 'Material': "{'title_ui': 'Material/Materiel', 'description': None}", 'IsEmergency': "{'title_ui': 'Akut', 'description': 'Akut-remiss. Fr.o.m. version 13.6 finns även akut på analysnivå i MultiLabOrders_Analyses'}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'ExternalUnitIdTypeCode': "{'title_ui': 'Id-typ', 'description': 'Id-typ för beställningen'}", 'OrdererCareUnitExternalID': "{'title_ui': 'B:', 'description': 'Kod för extern enhet för beställare'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'S:', 'description': 'Kod för extern enhet för svarsmottagare'}", 'InvoiceeCareUnitExternalID': "{'title_ui': 'F:', 'description': 'Kod för extern enhet för fakturamottagare'}", 'RecipientCareUnitExternalID': "{'title_ui': 'Lab-kod', 'description': 'Kod för extern enhet för labb'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CancellationComment] AS VARCHAR(MAX)) AS [CancellationComment],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([Disciplines] AS VARCHAR(MAX)) AS [Disciplines],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([ExternalUnitIdTypeCode] AS VARCHAR(MAX)) AS [ExternalUnitIdTypeCode],
		CAST([GUID] AS VARCHAR(MAX)) AS [GUID],
		CAST([InvoiceeCareUnitExternalID] AS VARCHAR(MAX)) AS [InvoiceeCareUnitExternalID],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsBloodInfection] AS VARCHAR(MAX)) AS [IsBloodInfection],
		CAST([IsElectronic] AS VARCHAR(MAX)) AS [IsElectronic],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([IsWatched] AS VARCHAR(MAX)) AS [IsWatched],
		CAST([LabOrderSettingsID] AS VARCHAR(MAX)) AS [LabOrderSettingsID],
		CAST([Material] AS VARCHAR(MAX)) AS [Material],
		CONVERT(varchar(max), [OrderDateTime], 126) AS [OrderDateTime],
		CAST([OrderedBy] AS VARCHAR(MAX)) AS [OrderedBy],
		CAST([OrdererCareUnitExternalID] AS VARCHAR(MAX)) AS [OrdererCareUnitExternalID],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([OrdererComment] AS VARCHAR(MAX)) AS [OrdererComment],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PlannedSamplingDate], 126) AS [PlannedSamplingDate],
		CONVERT(varchar(max), [PlannedSamplingTime], 126) AS [PlannedSamplingTime],
		CAST([RID] AS VARCHAR(MAX)) AS [RID],
		CAST([RecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [RecipientCareUnitExternalID],
		CAST([RecipientCareUnitKombika] AS VARCHAR(MAX)) AS [RecipientCareUnitKombika],
		CAST([RecipientSID] AS VARCHAR(MAX)) AS [RecipientSID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringDoctorUserName] AS VARCHAR(MAX)) AS [ReferringDoctorUserName],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([ReplyRecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitExternalID],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([SamplerComment] AS VARCHAR(MAX)) AS [SamplerComment],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingDatetime], 126) AS [SamplingDatetime],
		CAST([SamplingLocation] AS VARCHAR(MAX)) AS [SamplingLocation],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SavedStatusID] AS VARCHAR(MAX)) AS [SavedStatusID],
		CAST([SignedBy] AS VARCHAR(MAX)) AS [SignedBy],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version],
		CONVERT(varchar(max), [WatchDate], 126) AS [WatchDate],
		CAST([WatchStatusID] AS VARCHAR(MAX)) AS [WatchStatusID] 
	FROM Intelligence.viewreader.vMultiLabOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    