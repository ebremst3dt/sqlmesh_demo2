
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Svar Immunologi (Huddinge). Nedlagd, användes till 2006.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'LID': 'varchar(max)', 'LabResponsibleDoctor': 'varchar(max)', 'LaboratoryCareUnitID': 'varchar(max)', 'LaboratoryKombika': 'varchar(max)', 'OrderDocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'SamplingDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'LaboratoryCareUnitID': "{'title_ui': None, 'description': 'Laboratoriets vårdenhets-id'}", 'LaboratoryKombika': "{'title_ui': None, 'description': 'Laboratoriets kombika/EXID'}", 'ReplyTimestamp': "{'title_ui': 'Framställd', 'description': 'Tidpunkt då svar skickades'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Den vårdenhet dit svaret har skickats. Styr behörigheten.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Vid presentation i TC tas kombikakod/EXID för svarsmottagare i första hand från beställningen. Om beställning saknas visas svarets kombikakod/EXID.'}", 'OrderDocumentID': "{'title_ui': None, 'description': 'Dokument-id för beställning'}", 'LID': "{'title_ui': 'L:', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system'}", 'ReferralID': "{'title_ui': 'R:', 'description': 'RID (TC remiss-id).'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Vid presentation i TC tas remitterande läkare i första hand från beställningen. Om beställning saknas visas svarets remitterande läkare.'}", 'LabResponsibleDoctor': "{'title_ui': 'Asvarig läkare', 'description': 'Ansvarig läkare lab'}", 'SamplingDatetime': "{'title_ui': 'Provtagningsdatum', 'description': 'Vid presentation i TC tas provtagningstidpunkt i första hand från beställningen. Om beställning saknas visas svarets provtagningstidpunkt.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([LabResponsibleDoctor] AS VARCHAR(MAX)) AS [LabResponsibleDoctor],
		CAST([LaboratoryCareUnitID] AS VARCHAR(MAX)) AS [LaboratoryCareUnitID],
		CAST([LaboratoryKombika] AS VARCHAR(MAX)) AS [LaboratoryKombika],
		CAST([OrderDocumentID] AS VARCHAR(MAX)) AS [OrderDocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CONVERT(varchar(max), [ReplyTimestamp], 126) AS [ReplyTimestamp],
		CONVERT(varchar(max), [SamplingDatetime], 126) AS [SamplingDatetime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vImmLabRepliesIDALab) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    