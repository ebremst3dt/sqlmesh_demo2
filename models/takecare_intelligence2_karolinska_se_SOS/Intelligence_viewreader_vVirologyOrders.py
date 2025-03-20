
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Beställning Virologilabb Huddinge. Exakt hur data ser ut kan variera beroende på vilket gränssnitt som använts.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'BecameIllDate': 'varchar(max)', 'Category': 'varchar(max)', 'CategoryID': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EmergencyTelNo': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'MiscComment': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'OrdererComment': 'varchar(max)', 'PatientID': 'varchar(max)', 'PlannedSamplingDate': 'varchar(max)', 'PlannedSamplingTime': 'varchar(max)', 'RID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnit': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplerComment': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TrackingDate': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': 'Finns ej för gamla beställningar'}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet. Finns ej för gamla beställningar.'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet. Finns ej för gamla beställningar.'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering. Finns ej för gamla beställningar.'}", 'OrderedBy': "{'title_ui': 'Framställd av/Sparad av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd/Sparad', 'description': None}", 'TrackingDate': "{'title_ui': None, 'description': 'Bevakningsdatum. Används inte än, nu lagras datumet för den planerade provtagningstiden eller den faktiska provtagningstiden. Finns ej för gamla beställningar.'}", 'RID': "{'title_ui': 'R:', 'description': None}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig.'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID. Finns ej för gamla beställningar.'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare.'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Svarsmottagare vårdenhetsid'}", 'ReplyRecipientCareUnit': "{'title_ui': 'S:', 'description': 'Svarsmottagare i klartext'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'IsEmergency': "{'title_ui': 'Akut', 'description': None}", 'EmergencyTelNo': "{'title_ui': 'Tel/Sökare', 'description': None}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'BecameIllDate': "{'title_ui': 'Insjukningsdatum', 'description': None}", 'SamplingDate': "{'title_ui': 'Provtagningstid/Planerad prov.tid.', 'description': 'Provtagningsdatum eller planerat provt. datum om beställningen bara sparats.'}", 'SamplingTime': "{'title_ui': 'Provtagningstid/Planerad prov.tid.', 'description': 'Provtagningsdatum eller planerat provt. datum om beställningen bara sparats.'}", 'PlannedSamplingDate': "{'title_ui': 'Planerad prov.tid.', 'description': 'Innehåller alltid planerad provt. tidpunkt. Finns ej för gamla beställningar.'}", 'PlannedSamplingTime': "{'title_ui': 'Planerad prov.tid.', 'description': 'Innehåller alltid planerad provt. tidpunkt. Finns ej för gamla beställningar.'}", 'MiscComment': "{'title_ui': 'Kommentar', 'description': 'Innehåller bl.a. Kliniska upplysningar, Beställarens och Provtagarens kommentarer'}", 'OrdererComment': "{'title_ui': 'Beställarens kommentar', 'description': 'Endast nya beställningar. Visas bara i beställningsbilden'}", 'SamplerComment': "{'title_ui': 'Provtagarens kommentarer', 'description': 'Endast nya beställningar. Visas bara i beställningsbilden'}", 'CategoryID': "{'title_ui': 'Beställningens utgångspunkt', 'description': 'Id för vald kategori'}", 'Category': "{'title_ui': 'Beställningens utgångspunkt', 'description': 'Vald beställningskategori'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(BecameIllDate AS VARCHAR(MAX)) AS BecameIllDate,
		CAST(Category AS VARCHAR(MAX)) AS Category,
		CAST(CategoryID AS VARCHAR(MAX)) AS CategoryID,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(EmergencyTelNo AS VARCHAR(MAX)) AS EmergencyTelNo,
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(IsBloodInfection AS VARCHAR(MAX)) AS IsBloodInfection,
		CAST(IsEmergency AS VARCHAR(MAX)) AS IsEmergency,
		CAST(LaboratoryKombika AS VARCHAR(MAX)) AS LaboratoryKombika,
		CAST(MiscComment AS VARCHAR(MAX)) AS MiscComment,
		CONVERT(varchar(max), OrderDateTime, 126) AS OrderDateTime,
		CAST(OrderedBy AS VARCHAR(MAX)) AS OrderedBy,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(OrdererComment AS VARCHAR(MAX)) AS OrdererComment,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), PlannedSamplingDate, 126) AS PlannedSamplingDate,
		CONVERT(varchar(max), PlannedSamplingTime, 126) AS PlannedSamplingTime,
		CAST(RID AS VARCHAR(MAX)) AS RID,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReferringDoctorUserID AS VARCHAR(MAX)) AS ReferringDoctorUserID,
		CAST(ReferringDoctorUserName AS VARCHAR(MAX)) AS ReferringDoctorUserName,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(ReplyRecipientCareUnit AS VARCHAR(MAX)) AS ReplyRecipientCareUnit,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CAST(SamplerComment AS VARCHAR(MAX)) AS SamplerComment,
		CONVERT(varchar(max), SamplingDate, 126) AS SamplingDate,
		CONVERT(varchar(max), SamplingTime, 126) AS SamplingTime,
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
	FROM Intelligence.viewreader.vVirologyOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    