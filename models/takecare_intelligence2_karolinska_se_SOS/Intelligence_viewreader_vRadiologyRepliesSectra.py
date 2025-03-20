
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Röntgensvar Sectra (Södertälje, Visby och SÖS).""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Comment': 'varchar(max)', 'Diagnosis': 'varchar(max)', 'DocumentID': 'varchar(max)', 'History': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeCareUnitIdTypeCode': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'LabDictatingDoctor': 'varchar(max)', 'LabDictatingDoctorCode': 'varchar(max)', 'LabReferralID': 'varchar(max)', 'LabReferringDoctorCode': 'varchar(max)', 'LabResponsibleDoctor': 'varchar(max)', 'LabResponsibleDoctorCode': 'varchar(max)', 'LaboratoryCareUnitExternalID': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryCareUnitIdTypeCode': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitExternalID': 'varchar(max)', 'OrdererCareUnitIdTypeCode': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivedDatetime': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitIdTypeCode': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplySignedDate': 'varchar(max)', 'ReplySignedTime': 'varchar(max)', 'ReplyText': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ReplyType': 'varchar(max)', 'ReportNumber': 'varchar(max)', 'ReportSequence': 'varchar(max)', 'ReportSerialNumber': 'varchar(max)', 'SID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID'}", 'SID': "{'title_ui': None, 'description': 'Laboratoriets system-id'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'ReportNumber': "{'title_ui': 'Report Nr:', 'description': 'Report Nr i TC är ReportNumber-ReportSequence'}", 'ReportSequence': "{'title_ui': 'Report Nr:', 'description': 'Report Nr i TC är ReportNumber-ReportSequence'}", 'ReportSerialNumber': "{'title_ui': None, 'description': 'Radnummer i svarsfilen (Visas ej i TC)'}", 'LabReferralID': "{'title_ui': None, 'description': 'Labb RID (referens-id).'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (TC remiss-id). Ev. inledande nollor läggs endast till vid presentation i TC.'}", 'ReferralDate': "{'title_ui': 'Remisstid', 'description': 'Vid presentation i TC tas remisstid i första hand från beställningen. Om beställning saknas visas svarets remisstid.'}", 'LabReferringDoctorCode': "{'title_ui': None, 'description': 'Remitterande läkare. Labbets interna kod, visas ej i TC'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare'}", 'History': "{'title_ui': 'Anamnes', 'description': 'Vid presentation i TC tas anamnestexten i första hand från beställningen. Om beställning saknas visas svarets anamnes.'}", 'Diagnosis': "{'title_ui': 'Frågeställning', 'description': 'Vid presentation i TC tas diagnosen i första hand från beställningen. Om beställning saknas visas svarets diagnos.'}", 'ReceivedDatetime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tidpunkt då svar skickades'}", 'ReplySignedDate': "{'title_ui': 'Signerinstidpunkt', 'description': 'Signeringsdatum'}", 'ReplySignedTime': "{'title_ui': 'Signerinstidpunkt', 'description': 'Signeringstid'}", 'ReplyType': "{'title_ui': 'Preliminär/Slutsvar/Tilläggsvar/Avvisad', 'description': {'break': [None, None, None, None]}}", 'LabDictatingDoctorCode': "{'title_ui': None, 'description': 'Dikterande läkare lab. Labbets interna kod, visas ej i TC'}", 'LabDictatingDoctor': "{'title_ui': None, 'description': 'Dikterande läkare lab. visas ej i TC'}", 'LabResponsibleDoctorCode': "{'title_ui': 'Ansvarig läkare', 'description': 'Ansvarig/Signerande läkare lab. Labbets interna kod, visas ej i TC'}", 'LabResponsibleDoctor': "{'title_ui': 'Ansvarig läkare', 'description': 'Ansvarig/Signerande läkare lab'}", 'ReplyText': "{'title_ui': 'Utlåtande', 'description': 'Svarstext'}", 'MachineTime': "{'title_ui': None, 'description': 'Tidpunkt då svaret skapades i röntgensystemet'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'S:', 'description': 'Kod för svarsmottagare så som det skickades i filen'}", 'ReplyRecipientCareUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för svarsmottagande enhet, visas ej i TC'}", 'OrdererCareUnitExternalID': "{'title_ui': 'B:', 'description': 'Kod för beställare så som det skickades i filen'}", 'OrdererCareUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för beställare enhet, visas ej i TC'}", 'InvoiceeCareUnitExternalID': "{'title_ui': 'F:', 'description': 'Kod för fakturamottagare så som det skickades i filen'}", 'InvoiceeCareUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för fakturamottagande enhet, visas ej i TC'}", 'LaboratoryCareUnitExternalID': "{'title_ui': 'FRÅN', 'description': 'Kod för svarande enhet så som det skickades i filen'}", 'LaboratoryCareUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för svarande enhet, visas ej i TC'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(Diagnosis AS VARCHAR(MAX)) AS Diagnosis,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(History AS VARCHAR(MAX)) AS History,
		CAST(InvoiceeCareUnitExternalID AS VARCHAR(MAX)) AS InvoiceeCareUnitExternalID,
		CAST(InvoiceeCareUnitIdTypeCode AS VARCHAR(MAX)) AS InvoiceeCareUnitIdTypeCode,
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(LabDictatingDoctor AS VARCHAR(MAX)) AS LabDictatingDoctor,
		CAST(LabDictatingDoctorCode AS VARCHAR(MAX)) AS LabDictatingDoctorCode,
		CAST(LabReferralID AS VARCHAR(MAX)) AS LabReferralID,
		CAST(LabReferringDoctorCode AS VARCHAR(MAX)) AS LabReferringDoctorCode,
		CAST(LabResponsibleDoctor AS VARCHAR(MAX)) AS LabResponsibleDoctor,
		CAST(LabResponsibleDoctorCode AS VARCHAR(MAX)) AS LabResponsibleDoctorCode,
		CAST(LaboratoryCareUnitExternalID AS VARCHAR(MAX)) AS LaboratoryCareUnitExternalID,
		CAST(LaboratoryCareUnitID AS VARCHAR(MAX)) AS LaboratoryCareUnitID,
		CAST(LaboratoryCareUnitIdTypeCode AS VARCHAR(MAX)) AS LaboratoryCareUnitIdTypeCode,
		CAST(LaboratoryKombika AS VARCHAR(MAX)) AS LaboratoryKombika,
		CONVERT(varchar(max), MachineTime, 126) AS MachineTime,
		CAST(OrderDocumentID AS VARCHAR(MAX)) AS OrderDocumentID,
		CAST(OrdererCareUnitExternalID AS VARCHAR(MAX)) AS OrdererCareUnitExternalID,
		CAST(OrdererCareUnitIdTypeCode AS VARCHAR(MAX)) AS OrdererCareUnitIdTypeCode,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), ReceivedDatetime, 126) AS ReceivedDatetime,
		CONVERT(varchar(max), ReferralDate, 126) AS ReferralDate,
		CAST(ReferralID AS VARCHAR(MAX)) AS ReferralID,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReplyRecipientCareUnitExternalID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitExternalID,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitIdTypeCode AS VARCHAR(MAX)) AS ReplyRecipientCareUnitIdTypeCode,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CONVERT(varchar(max), ReplySignedDate, 126) AS ReplySignedDate,
		CONVERT(varchar(max), ReplySignedTime, 126) AS ReplySignedTime,
		CAST(ReplyText AS VARCHAR(MAX)) AS ReplyText,
		CONVERT(varchar(max), ReplyTimestamp, 126) AS ReplyTimestamp,
		CAST(ReplyType AS VARCHAR(MAX)) AS ReplyType,
		CAST(ReportNumber AS VARCHAR(MAX)) AS ReportNumber,
		CAST(ReportSequence AS VARCHAR(MAX)) AS ReportSequence,
		CAST(ReportSerialNumber AS VARCHAR(MAX)) AS ReportSerialNumber,
		CAST(SID AS VARCHAR(MAX)) AS SID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vRadiologyRepliesSectra) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    