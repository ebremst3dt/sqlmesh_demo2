
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Svar Patologi/Cytologi (Karolinska).",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'LID': 'varchar(max)', 'LabReceivedDate': 'varchar(max)', 'LabResponsibleDoctor': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'OrderText': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivedDatetime': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyStatus': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ReplyType': 'varchar(max)', 'ResultText1': 'varchar(max)', 'ResultText2': 'varchar(max)', 'SID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Händelsetid', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar'}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets/EXID kombika'}", 'SID': "{'title_ui': None, 'description': 'Laboratoriets system-id'}", 'ReplyType': "{'title_ui': None, 'description': {'break': [None, None]}}", 'ReplyStatus': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None]}}", 'LabReceivedDate': "{'title_ui': 'Ankomsttid lab', 'description': 'Datum då beställningen togs emot på labb.'}", 'MachineTime': "{'title_ui': 'Framställd', 'description': 'Maskintid'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tidpunkt då svar skickades'}", 'ReceivedDatetime': "{'title_ui': None, 'description': 'Tidpunkt då svaret togs emot'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:/Regnr', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system. Regnr identifierar materialet.'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (TC remiss-id).'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare. Vid presentation i TC tas remitterande läkare i första hand från beställningen. Om beställning saknas visas svarets remitterande läkare.'}", 'LabResponsibleDoctor': "{'title_ui': None, 'description': 'Ansvarig läkare lab'}", 'OrderText': "{'title_ui': None, 'description': 'Fri text från remissen'}", 'ResultText1': "{'title_ui': 'Utlåtande', 'description': 'Svarstext'}", 'ResultText2': "{'title_ui': 'Utlåtande', 'description': 'Svarstext'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CONVERT(varchar(max), [LabReceivedDate], 126) AS [LabReceivedDate],
		CAST([LabResponsibleDoctor] AS VARCHAR(MAX)) AS [LabResponsibleDoctor],
		CAST([LaboratoryCareUnitID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitID],
		CAST([LaboratoryKombika] AS VARCHAR(MAX)) AS [LaboratoryKombika],
		CONVERT(varchar(max), [MachineTime], 126) AS [MachineTime],
		CAST([OrderDocumentID] AS VARCHAR(MAX)) AS [OrderDocumentID],
		CAST([OrderText] AS VARCHAR(MAX)) AS [OrderText],
		CAST([OrdererCareUnitKombika] AS VARCHAR(MAX)) AS [OrdererCareUnitKombika],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [ReceivedDatetime], 126) AS [ReceivedDatetime],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([ReplyStatus] AS VARCHAR(MAX)) AS [ReplyStatus],
		CONVERT(varchar(max), [ReplyTimestamp], 126) AS [ReplyTimestamp],
		CAST([ReplyType] AS VARCHAR(MAX)) AS [ReplyType],
		CAST([ResultText1] AS VARCHAR(MAX)) AS [ResultText1],
		CAST([ResultText2] AS VARCHAR(MAX)) AS [ResultText2],
		CAST([SID] AS VARCHAR(MAX)) AS [SID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vPathologyCytologyReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    