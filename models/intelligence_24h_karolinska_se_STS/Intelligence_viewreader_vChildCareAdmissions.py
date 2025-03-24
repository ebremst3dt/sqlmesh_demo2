
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="BHV-inskrivningar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdmissionDate': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DischargeComment': 'varchar(max)', 'DischargeDate': 'varchar(max)', 'DischargeToEXID': 'varchar(max)', 'DischargeToID': 'varchar(max)', 'DischargeToName': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'RegistrationStatus': 'varchar(max)', 'ResponsibleUserID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatus': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'TimestampSaved': "{'title_ui': 'Sparad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Sparad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'TimestampCreated': "{'title_ui': 'Original skapat', 'description': None}", 'CreatedByUserID': "{'title_ui': 'Original skapat av', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'AdmissionDate': "{'title_ui': 'Inskrivning - Datum', 'description': None}", 'ResponsibleUserID': "{'title_ui': 'Inskrivning - Ansvarig', 'description': None}", 'DischargeDate': "{'title_ui': 'Utskrivning - Datum', 'description': None}", 'DischargeToID': "{'title_ui': 'Utskrivning - Till', 'description': 'ID för typ av mottagare för utskrivning från BHV'}", 'DischargeToName': "{'title_ui': 'Utskrivning - Namn', 'description': 'Namn på mottagare för utskrivning från BHV'}", 'DischargeToEXID': "{'title_ui': 'Utskrivning - Namn', 'description': {'break': None}}", 'DischargeComment': "{'title_ui': 'Utskrivning - Kommentar', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), [AdmissionDate], 126) AS [AdmissionDate],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([DischargeComment] AS VARCHAR(MAX)) AS [DischargeComment],
		CONVERT(varchar(max), [DischargeDate], 126) AS [DischargeDate],
		CAST([DischargeToEXID] AS VARCHAR(MAX)) AS [DischargeToEXID],
		CAST([DischargeToID] AS VARCHAR(MAX)) AS [DischargeToID],
		CAST([DischargeToName] AS VARCHAR(MAX)) AS [DischargeToName],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([RegistrationStatus] AS VARCHAR(MAX)) AS [RegistrationStatus],
		CAST([ResponsibleUserID] AS VARCHAR(MAX)) AS [ResponsibleUserID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vChildCareAdmissions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    