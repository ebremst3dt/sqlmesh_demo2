
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Virologisvar (Huddinge).",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BecameIllDate': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EmergencyTelNo': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'LID': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'Priority': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferralTime': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SID': 'varchar(max)', 'SamplingDatetime': 'varchar(max)', 'Section': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'SID': "{'title_ui': None, 'description': 'Laboratoriets system-id'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (TC remiss-id).'}", 'ReferralDate': "{'title_ui': None, 'description': 'Remissdatum'}", 'ReferralTime': "{'title_ui': None, 'description': 'Remisstid'}", 'Section': "{'title_ui': None, 'description': 'Anger vilket sektion på labbet som utförde provet'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare. Vid presentation i TC tas remitterande läkare i första hand från beställningen. Om beställning saknas visas svarets remitterande läkare.'}", 'SamplingDatetime': "{'title_ui': 'Provtagningstid', 'description': 'Provtagningstidpunkt. Vid presentation i TC tas provtagningstid i första hand från beställningen. Om beställning saknas visas svarets provtagningstid.'}", 'Priority': "{'title_ui': None, 'description': 'Prioritet.'}", 'EmergencyTelNo': "{'title_ui': None, 'description': 'Tel/Sökare'}", 'BecameIllDate': "{'title_ui': None, 'description': 'Insjukningsdatum'}", 'MachineTime': "{'title_ui': 'Framställd', 'description': 'Maskintid'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BecameIllDate] AS VARCHAR(MAX)) AS [BecameIllDate],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([EmergencyTelNo] AS VARCHAR(MAX)) AS [EmergencyTelNo],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([LaboratoryCareUnitID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitID],
		CONVERT(varchar(max), [MachineTime], 126) AS [MachineTime],
		CAST([OrderDocumentID] AS VARCHAR(MAX)) AS [OrderDocumentID],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Priority] AS VARCHAR(MAX)) AS [Priority],
		CONVERT(varchar(max), [ReferralDate], 126) AS [ReferralDate],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CONVERT(varchar(max), [ReferralTime], 126) AS [ReferralTime],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([SID] AS VARCHAR(MAX)) AS [SID],
		CONVERT(varchar(max), [SamplingDatetime], 126) AS [SamplingDatetime],
		CAST([Section] AS VARCHAR(MAX)) AS [Section],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vVirologyReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    