
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Undersökningar innehåller ett eller flera prov (grupper). Vid vissa undantag kan ett prov delas upp på flera olika undersökningar.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'InvestigationCode': 'varchar(max)', 'InvestigationComment': 'varchar(max)', 'InvestigationName': 'varchar(max)', 'MedicalApprovedSignature': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReplyStatus': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'InvestigationCode': "{'title_ui': 'Undersökning', 'description': 'Undersökningskod'}", 'InvestigationName': "{'title_ui': 'Undersökning', 'description': 'Undersökningsnamn'}", 'ReplyStatus': "{'title_ui': None, 'description': {'break': [None, None]}}", 'MedicalApprovedSignature': "{'title_ui': None, 'description': 'Signatur för medicinskt godkännande'}", 'InvestigationComment': "{'title_ui': 'Undersökningskommentar', 'description': 'Undersökningskommentar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(InvestigationCode AS VARCHAR(MAX)) AS InvestigationCode,
		CAST(InvestigationComment AS VARCHAR(MAX)) AS InvestigationComment,
		CAST(InvestigationName AS VARCHAR(MAX)) AS InvestigationName,
		CAST(MedicalApprovedSignature AS VARCHAR(MAX)) AS MedicalApprovedSignature,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReplyStatus AS VARCHAR(MAX)) AS ReplyStatus,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMultiLabReplies_Investigations) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    