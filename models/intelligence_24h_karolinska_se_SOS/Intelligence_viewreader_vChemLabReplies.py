
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kemlabbsvar. Exakt hur data ser ut kan variera mellan labbsystem. Omfattar fyra dokumenttyper, se kolumnen DocType.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'DocType': 'varchar(max)', 'DocumentID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsCopy': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'IsFinal': 'varchar(max)', 'LID': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivedDateTime': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'SID': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'TCFormNo': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Händelsetid', 'description': 'Tidpunkt då denna version sparades'}", 'DocType': "{'title_ui': None, 'description': 'Take Cares interna dokumenttyp. 9=CLab (används inte längre), 92=Labmaster, 93=FlexLab, 95=MediLab'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare.'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare. Innehåller kombika/EXID för kopiemottagare om svaret är en kopia.'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID'}", 'SID': "{'title_ui': None, 'description': 'Labbets system-id'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'TCFormNo': "{'title_ui': None, 'description': 'Index i filen TCFORMS'}", 'LID': "{'title_ui': 'L:', 'description': 'Labb-id, dvs. labbsvarets id i laboratoriets system'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'LID och remiss-id (RID) är ofta identiska'}", 'IsEmergency': "{'title_ui': 'Rutinsvar/Akutsvar', 'description': '0=Rutin 1=Akut'}", 'IsFinal': "{'title_ui': 'Delsvar/Slutsvar', 'description': '0=Delsvar 1=Slutsvar'}", 'IsCopy': "{'title_ui': 'KOPIA', 'description': '0=Original 1=Elektronisk kopia'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare. Fritext.'}", 'SamplingDate': "{'title_ui': 'Provtagningstid', 'description': 'Datum då provtagning skett'}", 'SamplingTime': "{'title_ui': 'Provtagningstid', 'description': 'Klockslag då provtagning skett'}", 'ReceivedDateTime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot. Ankomsttiden som visas i TC tas från vidimeringstransaktionen.'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tidpunkt då svar skickades'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DocType] AS VARCHAR(MAX)) AS [DocType],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsCopy] AS VARCHAR(MAX)) AS [IsCopy],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([IsFinal] AS VARCHAR(MAX)) AS [IsFinal],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([LaboratoryCareUnitID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitID],
		CAST([LaboratoryKombika] AS VARCHAR(MAX)) AS [LaboratoryKombika],
		CAST([OrderDocumentID] AS VARCHAR(MAX)) AS [OrderDocumentID],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [ReceivedDateTime], 126) AS [ReceivedDateTime],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CONVERT(varchar(max), [ReplyTimestamp], 126) AS [ReplyTimestamp],
		CAST([SID] AS VARCHAR(MAX)) AS [SID],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CAST([TCFormNo] AS VARCHAR(MAX)) AS [TCFormNo],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vChemLabReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    