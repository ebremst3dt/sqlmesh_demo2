
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Inskrivningsplanering",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ActivityID': 'varchar(max)', 'AdmissionDate': 'varchar(max)', 'AdmissionEmergencyTypeID': 'varchar(max)', 'AdmissionTime': 'varchar(max)', 'AdmissionplanComment': 'varchar(max)', 'AdmittingCareUnitID': 'varchar(max)', 'BedID': 'varchar(max)', 'BedName': 'varchar(max)', 'CarePlanDocumentID': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'RegistrationStatus': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatus': "{'title_ui': None, 'description': {'break': None}}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': None}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Original skapat av.'}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Synlig i lista över Inskrivningsplaneringar.'}", 'ActivityID': "{'title_ui': 'Aktivitetsid.', 'description': {'break': [None, None, None]}}", 'CarePlanDocumentID': "{'title_ui': 'Vårdplansid', 'description': 'Komponentnr (id) till vårdplan från vilken inskrivningsplaneringen skapades, eller Null om den skapades fristående.'}", 'AdmissionDate': "{'title_ui': 'Inskrivningsdatum', 'description': 'Datum planerad inskrivning.'}", 'AdmissionTime': "{'title_ui': 'Inskrivningsdatum', 'description': 'Tid för planerad inskrivning.'}", 'AdmissionEmergencyTypeID': "{'title_ui': 'Akut', 'description': {'break': None}}", 'AdmittingCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Vårdenhet pat. planeras skrivas in på eller Null om vårdenhet ej angivits.'}", 'BedID': '{\'title_ui\': None, \'description\': \'Förslag på sängplats (kod). Samma koder som i BedAssignments - lokalt per vårdenhet. Gemensamt för alla är -1 om aktivt val "Oplacerad" görs; Null om inget val gjorts.\'}', 'BedName': '{\'title_ui\': \'Vårdplats\', \'description\': \'Förslag på sängplats (namn). Samma namn som i BedAssignments - lokalt per vårdenhet. Null om aktivt val ej gjorts. Alternativet "Oplacerad" kan väljas aktivt inom alla vårdenheter.\'}', 'AdmissionplanComment': '{\'title_ui\': None, \'description\': \'Kommentar synlig i "Inskrivningsplanering".\'}', 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ActivityID] AS VARCHAR(MAX)) AS [ActivityID],
		CONVERT(varchar(max), [AdmissionDate], 126) AS [AdmissionDate],
		CAST([AdmissionEmergencyTypeID] AS VARCHAR(MAX)) AS [AdmissionEmergencyTypeID],
		CONVERT(varchar(max), [AdmissionTime], 126) AS [AdmissionTime],
		CAST([AdmissionplanComment] AS VARCHAR(MAX)) AS [AdmissionplanComment],
		CAST([AdmittingCareUnitID] AS VARCHAR(MAX)) AS [AdmittingCareUnitID],
		CAST([BedID] AS VARCHAR(MAX)) AS [BedID],
		CAST([BedName] AS VARCHAR(MAX)) AS [BedName],
		CAST([CarePlanDocumentID] AS VARCHAR(MAX)) AS [CarePlanDocumentID],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([RegistrationStatus] AS VARCHAR(MAX)) AS [RegistrationStatus],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vAdmissionPlans) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    