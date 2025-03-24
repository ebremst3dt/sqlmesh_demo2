
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Multilabsvar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'Disciplines': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'GUID': 'varchar(max)', 'HasHighPriority': 'varchar(max)', 'HasReplacingFinalAnalysis': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'LID': 'varchar(max)', 'LabComment': 'varchar(max)', 'LabReceivedDateTime': 'varchar(max)', 'LaboratoryCareUnitExternalID': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'MedicalAssessment': 'varchar(max)', 'NoOfAttachedFiles': 'varchar(max)', 'OrderComment': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitExternalID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivedDateTime': 'varchar(max)', 'RefOperatorIsActivated': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyAcknowledgeType': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyReference': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ReplyType': 'varchar(max)', 'SID': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': None, 'description': 'Version skapad'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare.'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare. Innehåller kombika/EXID för kopiemottagare om svaret är en kopia.'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare. NULL om fakturamottagare är samma som beställare.'}", 'LaboratoryCareUnitID': "{'title_ui': 'Svarande enhet', 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': 'Svarande enhet', 'description': 'Laboratoriets kombika/EXID'}", 'SID': "{'title_ui': None, 'description': 'Labbets system-id'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:', 'description': 'Labb-id, dvs. labbsvarets id i laboratoriets system'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'LID och remiss-id (RID) är ofta identiska'}", 'Disciplines': "{'title_ui': 'Disciplin', 'description': 'Discipliner i svaret'}", 'GUID': "{'title_ui': None, 'description': 'Unikt ID på beställningen'}", 'HasHighPriority': "{'title_ui': None, 'description': 'Prioriterat prov'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': None}", 'SamplingDate': "{'title_ui': 'Provtagn.tid', 'description': 'Datum då provtagning skett'}", 'SamplingTime': "{'title_ui': 'Provtagn.tid', 'description': 'Klockslag då provtagning skett'}", 'ReceivedDateTime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot. Ankomsttiden som visas i TC tas från vidimeringstransaktionen.'}", 'ReplyTimestamp': "{'title_ui': 'Framställd', 'description': 'Tidpunkt då svar skickades'}", 'ReplyType': "{'title_ui': 'Status', 'description': 'Statuskod'}", 'LabReceivedDateTime': "{'title_ui': None, 'description': 'Tidpunkt då beställningen togs emot på labb'}", 'OrderComment': "{'title_ui': 'Remisskommentar', 'description': 'Kommentar angående hela remissen'}", 'LabComment': "{'title_ui': 'Labkommentar', 'description': 'Utlåtande angående hela remissen'}", 'MedicalAssessment': "{'title_ui': 'Medicinsk bedömn', 'description': 'Medicinskt utlåtande angående hela remissen'}", 'ExternalUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för svaret'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'S:', 'description': 'Kod för svarsmottagare så som det skickades i filen'}", 'OrdererCareUnitExternalID': "{'title_ui': 'B:', 'description': 'Kod för beställare så som det skickades i filen'}", 'InvoiceeCareUnitExternalID': "{'title_ui': 'F:', 'description': 'Kod för fakturamottagare så som det skickades i filen'}", 'LaboratoryCareUnitExternalID': "{'title_ui': 'FRÅN', 'description': 'Kod för Svarande enhet så som det skickades i filen'}", 'ReplyReference': "{'title_ui': None, 'description': 'Referens för överföringen'}", 'ReplyAcknowledgeType': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'HasReplacingFinalAnalysis': "{'title_ui': None, 'description': 'Om någon analys är korrigerad'}", 'RefOperatorIsActivated': "{'title_ui': None, 'description': 'Om denna är satt så ersätts ReferenceArea-kolumnerna i analys-tabellen av de nya kolumnerna RefMin, RefMax, RefOperator och RefText'}", 'NoOfAttachedFiles': "{'title_ui': None, 'description': 'Hur många bifogade filer finns i detta svar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Disciplines] AS VARCHAR(MAX)) AS [Disciplines],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([ExternalUnitIdTypeCode] AS VARCHAR(MAX)) AS [ExternalUnitIdTypeCode],
		CAST([GUID] AS VARCHAR(MAX)) AS [GUID],
		CAST([HasHighPriority] AS VARCHAR(MAX)) AS [HasHighPriority],
		CAST([HasReplacingFinalAnalysis] AS VARCHAR(MAX)) AS [HasReplacingFinalAnalysis],
		CAST([InvoiceeCareUnitExternalID] AS VARCHAR(MAX)) AS [InvoiceeCareUnitExternalID],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([LabComment] AS VARCHAR(MAX)) AS [LabComment],
		CONVERT(varchar(max), [LabReceivedDateTime], 126) AS [LabReceivedDateTime],
		CAST([LaboratoryCareUnitExternalID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitExternalID],
		CAST([LaboratoryCareUnitID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitID],
		CAST([LaboratoryKombika] AS VARCHAR(MAX)) AS [LaboratoryKombika],
		CAST([MedicalAssessment] AS VARCHAR(MAX)) AS [MedicalAssessment],
		CAST([NoOfAttachedFiles] AS VARCHAR(MAX)) AS [NoOfAttachedFiles],
		CAST([OrderComment] AS VARCHAR(MAX)) AS [OrderComment],
		CAST([OrderDocumentID] AS VARCHAR(MAX)) AS [OrderDocumentID],
		CAST([OrdererCareUnitExternalID] AS VARCHAR(MAX)) AS [OrdererCareUnitExternalID],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [ReceivedDateTime], 126) AS [ReceivedDateTime],
		CAST([RefOperatorIsActivated] AS VARCHAR(MAX)) AS [RefOperatorIsActivated],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReplyAcknowledgeType] AS VARCHAR(MAX)) AS [ReplyAcknowledgeType],
		CAST([ReplyRecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitExternalID],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([ReplyReference] AS VARCHAR(MAX)) AS [ReplyReference],
		CONVERT(varchar(max), [ReplyTimestamp], 126) AS [ReplyTimestamp],
		CAST([ReplyType] AS VARCHAR(MAX)) AS [ReplyType],
		CAST([SID] AS VARCHAR(MAX)) AS [SID],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMultiLabReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    