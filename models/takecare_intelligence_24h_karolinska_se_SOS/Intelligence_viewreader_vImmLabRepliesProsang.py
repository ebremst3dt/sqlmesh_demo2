
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Svar Immunologilabb Karolinska.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'GroupComment': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'LabReferralID': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'Priority': 'varchar(max)', 'ReceivedDatetime': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyText': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ReplyType': 'varchar(max)', 'ReportComment': 'varchar(max)', 'ReportDateTime': 'varchar(max)', 'SID': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'SID': "{'title_ui': None, 'description': 'Laboratoriets system-id'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LabReferralID': "{'title_ui': 'L:', 'description': 'Labb RID (referens-id).'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (TC remiss-id).'}", 'SamplingDate': "{'title_ui': 'Provtagningsdatum', 'description': 'Datum då provtagning skett'}", 'SamplingTime': "{'title_ui': 'Provtagningsdatum', 'description': 'Klockslag då provtagning skett'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare'}", 'ReceivedDatetime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tidpunkt då svar skickades'}", 'ReportDateTime': "{'title_ui': 'Framställd', 'description': 'Tidpunkt då svaret framställts på labb'}", 'ReplyType': "{'title_ui': 'Preliminär/Slutsvar', 'description': {'break': [None, None]}}", 'Priority': "{'title_ui': None, 'description': {'break': [None, None]}}", 'ReplyText': "{'title_ui': None, 'description': 'Svarstext'}", 'ReportComment': "{'title_ui': None, 'description': 'Kommentar från labbet angående hela svaret.'}", 'GroupComment': "{'title_ui': None, 'description': 'Kommentar från labbet angående analysresultatet.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(GroupComment AS VARCHAR(MAX)) AS GroupComment,
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(LabReferralID AS VARCHAR(MAX)) AS LabReferralID,
		CAST(LaboratoryCareUnitID AS VARCHAR(MAX)) AS LaboratoryCareUnitID,
		CAST(OrderDocumentID AS VARCHAR(MAX)) AS OrderDocumentID,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Priority AS VARCHAR(MAX)) AS Priority,
		CONVERT(varchar(max), ReceivedDatetime, 126) AS ReceivedDatetime,
		CAST(ReferralID AS VARCHAR(MAX)) AS ReferralID,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CAST(ReplyText AS VARCHAR(MAX)) AS ReplyText,
		CONVERT(varchar(max), ReplyTimestamp, 126) AS ReplyTimestamp,
		CAST(ReplyType AS VARCHAR(MAX)) AS ReplyType,
		CAST(ReportComment AS VARCHAR(MAX)) AS ReportComment,
		CONVERT(varchar(max), ReportDateTime, 126) AS ReportDateTime,
		CAST(SID AS VARCHAR(MAX)) AS SID,
		CONVERT(varchar(max), SamplingDate, 126) AS SamplingDate,
		CONVERT(varchar(max), SamplingTime, 126) AS SamplingTime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vImmLabRepliesProsang) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    