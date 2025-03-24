
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Varför en plan bör utföras.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompletedDate': 'varchar(max)', 'CompletedTime': 'varchar(max)', 'Description': 'varchar(max)', 'DocumentID': 'varchar(max)', 'InitiatedDate': 'varchar(max)', 'InitiatedTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProblemTypeID': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSavedGPE': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': 'Aktivitetplansid', 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'ProblemTypeID': "{'title_ui': 'Typ', 'description': 'Vilken typ av problem som föranledde planen: resurs, risk, problem etc'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Description': "{'title_ui': 'Beskrivning', 'description': 'En närmare beskrivning av målet/problemet/utvärderingen'}", 'InitiatedDate': "{'title_ui': 'Påbörjad', 'description': None}", 'InitiatedTime': "{'title_ui': 'Påbörjad', 'description': None}", 'CompletedDate': "{'title_ui': 'Avslutad', 'description': None}", 'CompletedTime': "{'title_ui': 'Avslutad', 'description': None}", 'TimestampSavedGPE': "{'title_ui': 'Ändrad', 'description': 'När målet/problemet/utvärderingen registrerades, maskintid.'}", 'SavedByUserID': "{'title_ui': 'Ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Ändrad på', 'description': 'Den vårdenhet där användaren gjorde registreringen.'}", 'ProfessionID': "{'title_ui': 'Yrke', 'description': 'Den yrkesgrupp användaren hade vid registreringen.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), [CompletedDate], 126) AS [CompletedDate],
		CONVERT(varchar(max), [CompletedTime], 126) AS [CompletedTime],
		CAST([Description] AS VARCHAR(MAX)) AS [Description],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [InitiatedDate], 126) AS [InitiatedDate],
		CONVERT(varchar(max), [InitiatedTime], 126) AS [InitiatedTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ProblemTypeID] AS VARCHAR(MAX)) AS [ProblemTypeID],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSavedGPE], 126) AS [TimestampSavedGPE],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vTaskPlans_ResourceRiskProblem) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    