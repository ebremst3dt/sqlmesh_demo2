
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställning Immunologi Huddinge. Exakt hur data ser ut kan variera beroende på vilket gränssnitt som använts. Nedlagd, användes till 2006.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AllergyDiagnosis': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExposureAllergen': 'varchar(max)', 'HasAllergenExposure': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsLabDecision': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderTypeID': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'OtherInfo': 'varchar(max)', 'PatientID': 'varchar(max)', 'RID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'OrderedBy': "{'title_ui': 'Beställd av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd/Sparad', 'description': None}", 'OrderTypeID': "{'title_ui': None, 'description': {'break': None}}", 'RID': "{'title_ui': 'R:', 'description': None}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig.'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Svarsmottagare vårdenhetsid'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'SamplingDate': "{'title_ui': 'Provtagningsdatum', 'description': 'Provtagningsdatum'}", 'SamplingTime': "{'title_ui': 'Provtagningsdatum', 'description': 'Provtagningstid'}", 'AllergyDiagnosis': "{'title_ui': 'Har pat. tidigare fått diagnosen Atopiker?', 'description': {'break': [None, None, None, None]}}", 'HasAllergenExposure': "{'title_ui': 'Extrem allergenexposition i patientens närmiljö', 'description': 'Endast Allergi (OrderTypeID=1)'}", 'ExposureAllergen': "{'title_ui': 'Mot vad?', 'description': {'break': None}}", 'OtherInfo': "{'title_ui': 'Övriga upplysningar', 'description': None}", 'IsLabDecision': "{'title_ui': 'Jag överlåter till laboratoriet att välja analyser med ledning av gjorda val', 'description': 'Endast Autoimmunitet (OrderTypeID=0)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AllergyDiagnosis] AS VARCHAR(MAX)) AS [AllergyDiagnosis],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([ExposureAllergen] AS VARCHAR(MAX)) AS [ExposureAllergen],
		CAST([HasAllergenExposure] AS VARCHAR(MAX)) AS [HasAllergenExposure],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsLabDecision] AS VARCHAR(MAX)) AS [IsLabDecision],
		CONVERT(varchar(max), [OrderDateTime], 126) AS [OrderDateTime],
		CAST([OrderTypeID] AS VARCHAR(MAX)) AS [OrderTypeID],
		CAST([OrderedBy] AS VARCHAR(MAX)) AS [OrderedBy],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([OtherInfo] AS VARCHAR(MAX)) AS [OtherInfo],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([RID] AS VARCHAR(MAX)) AS [RID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringDoctorUserName] AS VARCHAR(MAX)) AS [ReferringDoctorUserName],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vImmLabOrdersIDALab) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    