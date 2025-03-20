
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Svar från Fysiologikiniken. (Karolinska Huddinge och Solna)""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Exam': 'varchar(max)', 'ExamEndDate': 'varchar(max)', 'ExamEndTime': 'varchar(max)', 'ExamStartDate': 'varchar(max)', 'ExamStartTime': 'varchar(max)', 'ExaminationDate': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'LID': 'varchar(max)', 'LabDoctor': 'varchar(max)', 'LabReferralID': 'varchar(max)', 'LabResponsibleDoctor1': 'varchar(max)', 'LabResponsibleDoctor2': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'LocationSID': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientHeight': 'varchar(max)', 'PatientID': 'varchar(max)', 'PatientWeight': 'varchar(max)', 'Priority': 'varchar(max)', 'ReceivedDatetime': 'varchar(max)', 'ReferralArrivalDate': 'varchar(max)', 'ReferralArrivalTime': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferralTime': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplySigned': 'varchar(max)', 'ReplyText': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ReplyType': 'varchar(max)', 'ReportNumber': 'varchar(max)', 'ReportSequence': 'varchar(max)', 'SID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID'}", 'SID': "{'title_ui': None, 'description': 'Laboratoriets system-id'}", 'LocationSID': "{'title_ui': None, 'description': 'Mottagarens SID'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system'}", 'LabReferralID': "{'title_ui': None, 'description': 'Labb RID (referens-id). Visas upp efter R: om ReferralID saknas.'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (TC remiss-id).'}", 'ReferralDate': "{'title_ui': 'Remissdatum', 'description': 'Vid presentation i TC tas remissdatum i första hand från beställningen. Om beställning saknas visas svarets remissdatum.'}", 'ReferralTime': "{'title_ui': 'Remissdatum', 'description': 'Vid presentation i TC tas remissdatum i första hand från beställningen. Om beställning saknas visas svarets remissdatum.'}", 'ReferralArrivalDate': "{'title_ui': 'Ankomsttid Lab', 'description': 'Visas om beställning och svarets remissdatum saknas.'}", 'ReferralArrivalTime': "{'title_ui': 'Ankomsttid Lab', 'description': 'Visas om beställning och svarets remissdatum saknas.'}", 'PatientHeight': "{'title_ui': 'Längd', 'description': 'Endast Huddinge'}", 'PatientWeight': "{'title_ui': 'Vikt', 'description': 'Endast Huddinge'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Vid presentation i TC tas remitterande läkare i första hand från beställningen. Om beställning saknas visas svarets remitterande läkare.'}", 'ExamStartDate': "{'title_ui': None, 'description': 'Datum då undersökningen startade'}", 'ExamStartTime': "{'title_ui': None, 'description': 'Tid då undersökningen startade'}", 'ExamEndDate': '{\'title_ui\': \'Datum/tid och Undersökningsdatum\', \'description\': \'Datum då undersökningen slutade. Om datumet saknas, tas det istället från maskintiden (framställd) Visas ej som "Undersökningsdatum" i TakeCare om svaret kommer från Huddinge fyslab.\'}', 'ExamEndTime': "{'title_ui': 'Datum/tid', 'description': 'Tid då undersökningen slutade. Om tiden saknas,tas den istället från maskintiden (framställd)'}", 'ReceivedDatetime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tidpunkt då svar skickades'}", 'ReplySigned': "{'title_ui': None, 'description': 'Signeringsstatus'}", 'ReplyType': "{'title_ui': 'Preliminär/Avvisad/Slutsvar/Tilläggssvar', 'description': {'break': [None, None, None, None]}}", 'Priority': "{'title_ui': None, 'description': 'Prioritet (Kommer från labbsystemet, men används inte i TC)'}", 'LabResponsibleDoctor1': "{'title_ui': None, 'description': 'Ansvarig läkare labb'}", 'LabResponsibleDoctor2': "{'title_ui': 'Läkare', 'description': 'Ansvarig läkare labb. Visas ej i TakeCare om svaret kommer från Huddinge fyslab.'}", 'MachineTime': "{'title_ui': 'Framställd', 'description': 'Maskintid'}", 'ReportNumber': "{'title_ui': 'L: innan streck', 'description': 'Används som LID i TakeCare. Endast Sectra.'}", 'ReportSequence': "{'title_ui': 'L: efter streck', 'description': 'Används för att garantera att instanser av samma svar kommer i rätt ordning. Endast Sectra.'}", 'Exam': "{'title_ui': 'Undersökning', 'description': 'Endast Huddinge'}", 'ReplyText': "{'title_ui': None, 'description': 'Svarstext'}", 'LabDoctor': "{'title_ui': 'Läkare', 'description': 'Innehåller Namn, yrke och labbets interna signatur. Endast Huddinge'}", 'ExaminationDate': "{'title_ui': 'Undersökningsdatum', 'description': 'Endast Huddinge'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Exam AS VARCHAR(MAX)) AS Exam,
		CONVERT(varchar(max), ExamEndDate, 126) AS ExamEndDate,
		CONVERT(varchar(max), ExamEndTime, 126) AS ExamEndTime,
		CONVERT(varchar(max), ExamStartDate, 126) AS ExamStartDate,
		CONVERT(varchar(max), ExamStartTime, 126) AS ExamStartTime,
		CONVERT(varchar(max), ExaminationDate, 126) AS ExaminationDate,
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(LID AS VARCHAR(MAX)) AS LID,
		CAST(LabDoctor AS VARCHAR(MAX)) AS LabDoctor,
		CAST(LabReferralID AS VARCHAR(MAX)) AS LabReferralID,
		CAST(LabResponsibleDoctor1 AS VARCHAR(MAX)) AS LabResponsibleDoctor1,
		CAST(LabResponsibleDoctor2 AS VARCHAR(MAX)) AS LabResponsibleDoctor2,
		CAST(LaboratoryCareUnitID AS VARCHAR(MAX)) AS LaboratoryCareUnitID,
		CAST(LaboratoryKombika AS VARCHAR(MAX)) AS LaboratoryKombika,
		CAST(LocationSID AS VARCHAR(MAX)) AS LocationSID,
		CONVERT(varchar(max), MachineTime, 126) AS MachineTime,
		CAST(OrderDocumentID AS VARCHAR(MAX)) AS OrderDocumentID,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(PatientHeight AS VARCHAR(MAX)) AS PatientHeight,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PatientWeight AS VARCHAR(MAX)) AS PatientWeight,
		CAST(Priority AS VARCHAR(MAX)) AS Priority,
		CONVERT(varchar(max), ReceivedDatetime, 126) AS ReceivedDatetime,
		CONVERT(varchar(max), ReferralArrivalDate, 126) AS ReferralArrivalDate,
		CONVERT(varchar(max), ReferralArrivalTime, 126) AS ReferralArrivalTime,
		CONVERT(varchar(max), ReferralDate, 126) AS ReferralDate,
		CAST(ReferralID AS VARCHAR(MAX)) AS ReferralID,
		CONVERT(varchar(max), ReferralTime, 126) AS ReferralTime,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CAST(ReplySigned AS VARCHAR(MAX)) AS ReplySigned,
		CAST(ReplyText AS VARCHAR(MAX)) AS ReplyText,
		CONVERT(varchar(max), ReplyTimestamp, 126) AS ReplyTimestamp,
		CAST(ReplyType AS VARCHAR(MAX)) AS ReplyType,
		CAST(ReportNumber AS VARCHAR(MAX)) AS ReportNumber,
		CAST(ReportSequence AS VARCHAR(MAX)) AS ReportSequence,
		CAST(SID AS VARCHAR(MAX)) AS SID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPhysiologyReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    