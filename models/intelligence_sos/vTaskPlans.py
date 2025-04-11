
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="En aktivitetsplan är en mall för vilka aktiviteter som behöver göras för en patient. Aktiviteterna kan ha ett mål och resultatet kan utvärderas med fritext.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'Description': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EndDate': 'varchar(max)', 'EndTime': 'varchar(max)', 'GoalDescription': 'varchar(max)', 'GoalEvaluation': 'varchar(max)', 'IsCurrent': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProblemDescription': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'ResponsibleUserID': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'StartDate': 'varchar(max)', 'StartTime': 'varchar(max)', 'Status': 'varchar(max)', 'TaskPlanTermID': 'varchar(max)', 'TemplateID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': 'Aktivitetplansid', 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Vårdenhet som dokumentet senast sparades på.'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Användare som skapat dokumentet från början.'}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'Status': "{'title_ui': 'Makulerad', 'description': {'break': None}}", 'TemplateID': "{'title_ui': None, 'description': 'Om planen skapades från en fördefinierad mall är detta den mallens id, annars NULL.'}", 'TaskPlanTermID': "{'title_ui': None, 'description': 'Avstängd hösten 2003, numera ges beskrivningen/namnet direkt från användaren eller mallen.'}", 'Description': "{'title_ui': 'Beskrivning', 'description': 'Innan hösten 2003 var detta namnet på termen som gav planen sitt namn, numera bara fritext som användaren kan redigera.'}", 'IsCurrent': "{'title_ui': None, 'description': 'Sant om planen är aktuell (annars avslutad)'}", 'StartDate': "{'title_ui': 'Påbörjad datum', 'description': None}", 'StartTime': "{'title_ui': 'Påbörjad datum', 'description': None}", 'EndDate': '{\'title_ui\': \'Slut\', \'description\': \'Datum som användaren angett att ett mål avslutades, registrerades tom hösten 2003. Sätts numera när användaren väljer status "Avslutad".\'}', 'EndTime': '{\'title_ui\': \'Slut\', \'description\': \'Tid som användaren angett att ett mål avslutades, registrerades tom hösten 2003. Sätts numera när användaren väljer status "Avslutad".\'}', 'ResponsibleUserID': "{'title_ui': 'Ansvarig', 'description': None}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Den yrkesgrupp som är ansvarig för att planen genomförs.'}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'ProblemDescription': "{'title_ui': 'Problembeskrivning', 'description': 'Fritext, registrerades tom hösten 2003'}", 'GoalDescription': "{'title_ui': 'Målbeskrivning', 'description': 'Fritext, registrerades tom hösten 2003'}", 'GoalEvaluation': "{'title_ui': 'Målutvärdering', 'description': 'Fritext, registrerades tom hösten 2003'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID', 'Version']
    ),
    cron="@daily",
    start=start,
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
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([Description] AS VARCHAR(MAX)) AS [Description],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [EndDate], 126) AS [EndDate],
		CONVERT(varchar(max), [EndTime], 126) AS [EndTime],
		CAST([GoalDescription] AS VARCHAR(MAX)) AS [GoalDescription],
		CAST([GoalEvaluation] AS VARCHAR(MAX)) AS [GoalEvaluation],
		CAST([IsCurrent] AS VARCHAR(MAX)) AS [IsCurrent],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ProblemDescription] AS VARCHAR(MAX)) AS [ProblemDescription],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([ResponsibleUserID] AS VARCHAR(MAX)) AS [ResponsibleUserID],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [StartDate], 126) AS [StartDate],
		CONVERT(varchar(max), [StartTime], 126) AS [StartTime],
		CAST([Status] AS VARCHAR(MAX)) AS [Status],
		CAST([TaskPlanTermID] AS VARCHAR(MAX)) AS [TaskPlanTermID],
		CAST([TemplateID] AS VARCHAR(MAX)) AS [TemplateID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vTaskPlans) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    