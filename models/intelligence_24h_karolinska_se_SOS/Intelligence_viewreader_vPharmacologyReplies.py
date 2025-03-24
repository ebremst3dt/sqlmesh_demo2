
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Farmakologisvar. Svar t.o.m. 2004 kom från CLab, därefter från SafirLis.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsCopy': 'varchar(max)', 'LID': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivedDateTime': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Händelsetid', 'description': 'Senaste version skapad'}", 'MachineTime': "{'title_ui': 'Framställd', 'description': 'Maskintid. Tidpunkt då svaret skapades på labb.'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare.'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare. Innehåller kombika/EXID för kopiemottagare om svaret är en kopia.'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare. NULL om fakturamottagare är samma som beställare.'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID'}", 'IsCopy': "{'title_ui': 'KOPIA', 'description': {'break': None}}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:', 'description': 'Labb-id för det senaste provtagningstillfället'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'LID och remiss-id (RID) är ofta identiska'}", 'ReferringDoctor': "{'title_ui': None, 'description': 'Remittentens namn'}", 'SamplingDate': "{'title_ui': 'Provtagningstid', 'description': 'Datum då provtagning skett'}", 'SamplingTime': "{'title_ui': 'Provtagningstid', 'description': 'Klockslag då provtagning skett'}", 'ReceivedDateTime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot. Ankomsttiden som visas i TC tas från vidimeringstransaktionen.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(IsCopy AS VARCHAR(MAX)) AS IsCopy,
		CAST(LID AS VARCHAR(MAX)) AS LID,
		CAST(LaboratoryCareUnitID AS VARCHAR(MAX)) AS LaboratoryCareUnitID,
		CAST(LaboratoryKombika AS VARCHAR(MAX)) AS LaboratoryKombika,
		CONVERT(varchar(max), MachineTime, 126) AS MachineTime,
		CAST(OrderDocumentID AS VARCHAR(MAX)) AS OrderDocumentID,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), ReceivedDateTime, 126) AS ReceivedDateTime,
		CAST(ReferralID AS VARCHAR(MAX)) AS ReferralID,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CONVERT(varchar(max), SamplingDate, 126) AS SamplingDate,
		CONVERT(varchar(max), SamplingTime, 126) AS SamplingTime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPharmacologyReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    