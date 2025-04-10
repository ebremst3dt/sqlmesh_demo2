
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Mikrolabbsvar.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisReference': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Examination': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsMissingSamplingTime': 'varchar(max)', 'LID': 'varchar(max)', 'LIDTCInternal': 'varchar(max)', 'LabReceivedDate': 'varchar(max)', 'LabReceivedTime': 'varchar(max)', 'LabResponsibleDoctor': 'varchar(max)', 'LaboratoryAddress': 'varchar(max)', 'LaboratoryCareUnit': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererAddress': 'varchar(max)', 'PatientID': 'varchar(max)', 'Priority': 'varchar(max)', 'ReceivedDatetime': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyDate': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyTime': 'varchar(max)', 'ReplyType': 'varchar(max)', 'SID': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'Specimen1': 'varchar(max)', 'Specimen2': 'varchar(max)', 'TCFormNo': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'TCFormNo': "{'title_ui': None, 'description': 'Index i filen TCFORMS.'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID'}", 'SID': "{'title_ui': None, 'description': 'Laboratoriets system-id'}", 'LaboratoryAddress': "{'title_ui': None, 'description': 'Laboratoriets adress. Kommer från labbsystemet.'}", 'LaboratoryCareUnit': "{'title_ui': 'Från', 'description': 'Laboratoriets namn'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'OrdererAddress': "{'title_ui': None, 'description': 'Beställarens adress. Kommer från labbsystemet.'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system'}", 'LIDTCInternal': "{'title_ui': None, 'description': 'Internt labb-id för att koppla ihop flera versioner av samma svar i TC'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (remiss-id). LID och RID är ofta identiska'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare'}", 'LabResponsibleDoctor': "{'title_ui': 'Ansv.lab.läkare', 'description': 'Ansvarig läkare lab'}", 'SamplingDate': "{'title_ui': 'Provtagningstid', 'description': 'Datum då provtagning skett'}", 'SamplingTime': "{'title_ui': 'Provtagningstid', 'description': 'Klockslag då provtagning skett'}", 'IsMissingSamplingTime': '{\'title_ui\': None, \'description\': \'Varningsflagga om provtagninstiden saknas i filen. 1=provtagninstiden saknas i filen. Provtagningstiden sätts till "Okänd" vid presentation av svaret på skärmen.\'}', 'ReceivedDatetime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot'}", 'ReplyDate': "{'title_ui': 'Svarstid', 'description': 'Datum då svar skickades'}", 'ReplyTime': "{'title_ui': 'Svarstid', 'description': 'Tidpunkt då svar skickades, ibland lagras endast datum'}", 'ReplyType': "{'title_ui': 'Slutsvar/Preliminärt', 'description': {'break': [None, None, None, None]}}", 'LabReceivedDate': "{'title_ui': 'Ankomsttid lab', 'description': 'Datum då beställningen togs emot på labb.'}", 'LabReceivedTime': "{'title_ui': 'Ankomsttid lab', 'description': 'Tidpunkt då beställningen togs emot på labb, ibland lagras endast datum'}", 'Examination': "{'title_ui': 'Undersökning', 'description': None}", 'Specimen1': "{'title_ui': 'Provmaterial', 'description': None}", 'Specimen2': "{'title_ui': 'Provmaterial', 'description': None}", 'Priority': "{'title_ui': None, 'description': 'Är troligtvis prioritet. Kommer från labbsystemet, men används inte i TC.'}", 'AnalysisReference': "{'title_ui': None, 'description': 'Provreferens. Kommer från labbsystemet. Är samma som LID, men med en extension (t.ex :OB).'}", 'MachineTime': "{'title_ui': None, 'description': 'Maskintid'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([AnalysisReference] AS VARCHAR(MAX)) AS [AnalysisReference],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Examination] AS VARCHAR(MAX)) AS [Examination],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsMissingSamplingTime] AS VARCHAR(MAX)) AS [IsMissingSamplingTime],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([LIDTCInternal] AS VARCHAR(MAX)) AS [LIDTCInternal],
		CONVERT(varchar(max), [LabReceivedDate], 126) AS [LabReceivedDate],
		CONVERT(varchar(max), [LabReceivedTime], 126) AS [LabReceivedTime],
		CAST([LabResponsibleDoctor] AS VARCHAR(MAX)) AS [LabResponsibleDoctor],
		CAST([LaboratoryAddress] AS VARCHAR(MAX)) AS [LaboratoryAddress],
		CAST([LaboratoryCareUnit] AS VARCHAR(MAX)) AS [LaboratoryCareUnit],
		CAST([LaboratoryCareUnitID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitID],
		CAST([LaboratoryKombika] AS VARCHAR(MAX)) AS [LaboratoryKombika],
		CONVERT(varchar(max), [MachineTime], 126) AS [MachineTime],
		CAST([OrderDocumentID] AS VARCHAR(MAX)) AS [OrderDocumentID],
		CAST([OrdererAddress] AS VARCHAR(MAX)) AS [OrdererAddress],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Priority] AS VARCHAR(MAX)) AS [Priority],
		CONVERT(varchar(max), [ReceivedDatetime], 126) AS [ReceivedDatetime],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CONVERT(varchar(max), [ReplyDate], 126) AS [ReplyDate],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CONVERT(varchar(max), [ReplyTime], 126) AS [ReplyTime],
		CAST([ReplyType] AS VARCHAR(MAX)) AS [ReplyType],
		CAST([SID] AS VARCHAR(MAX)) AS [SID],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CAST([Specimen1] AS VARCHAR(MAX)) AS [Specimen1],
		CAST([Specimen2] AS VARCHAR(MAX)) AS [Specimen2],
		CAST([TCFormNo] AS VARCHAR(MAX)) AS [TCFormNo],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMicroLabReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    