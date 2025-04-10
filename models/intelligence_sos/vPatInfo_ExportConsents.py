
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Samtycke till journaldataexport.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DecidedByUserID': 'varchar(max)', 'DecidedByUserName': 'varchar(max)', 'DecidedDate': 'varchar(max)', 'ExternalSystemID': 'varchar(max)', 'IsCurrent': 'varchar(max)', 'ObtainedByUserID': 'varchar(max)', 'ObtainedByUserName': 'varchar(max)', 'ObtainedDate': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'Status': 'varchar(max)', 'StudyLeaveReason': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ExternalSystemID': "{'title_ui': 'Till', 'description': 'Mottagare av journaldata (utanför TakeCare, ex. kvalitetsregister)'}", 'ObtainedByUserID': "{'title_ui': 'Inhämtat av', 'description': 'Den användare som inhämtat patientens samtycke (fr.o.m. sommaren 2007)'}", 'ObtainedByUserName': "{'title_ui': 'Inhämtat av', 'description': 'Den användare som inhämtat patientens samtycke (användarnamn)'}", 'ObtainedDate': "{'title_ui': 'Datum inhämtat', 'description': None}", 'DecidedByUserID': "{'title_ui': 'Ansvarig för beslut', 'description': 'Den användare som fattat beslutet (fr.o.m. sommaren 2007)'}", 'DecidedByUserName': "{'title_ui': 'Ansvarig för beslut', 'description': 'Den användare som fattat beslutet (användarnamn)'}", 'DecidedDate': "{'title_ui': 'Datum beslut', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Tidsbegränsad t.o.m.', 'description': 'Samtycket gäller till detta datum. Saknas datum gäller samtycket tills vidare.'}", 'TimestampSaved': "{'title_ui': 'Registrerad', 'description': 'Tidpunkten då samtycket registrerades i systemet'}", 'SavedByUserID': "{'title_ui': 'Registrerad av', 'description': 'Den användare som registrerat samtycket i systemet'}", 'SavedAtCareUnitID': "{'title_ui': 'Registrerad på', 'description': 'Där data är registrerat'}", 'IsCurrent': "{'title_ui': '(Ej) aktuell', 'description': 'Om samtycket fortfarande gäller'}", 'StudyLeaveReason': "{'title_ui': 'Orsak ej aktuell', 'description': 'Orsak till avslutande av samtycke'}", 'Status': "{'title_ui': None, 'description': 'Status för samtycket där 0 = Normal och -2 = Makulerad'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['PatientID', 'Row']
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
		CAST([DecidedByUserID] AS VARCHAR(MAX)) AS [DecidedByUserID],
		CAST([DecidedByUserName] AS VARCHAR(MAX)) AS [DecidedByUserName],
		CONVERT(varchar(max), [DecidedDate], 126) AS [DecidedDate],
		CAST([ExternalSystemID] AS VARCHAR(MAX)) AS [ExternalSystemID],
		CAST([IsCurrent] AS VARCHAR(MAX)) AS [IsCurrent],
		CAST([ObtainedByUserID] AS VARCHAR(MAX)) AS [ObtainedByUserID],
		CAST([ObtainedByUserName] AS VARCHAR(MAX)) AS [ObtainedByUserName],
		CONVERT(varchar(max), [ObtainedDate], 126) AS [ObtainedDate],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([Status] AS VARCHAR(MAX)) AS [Status],
		CAST([StudyLeaveReason] AS VARCHAR(MAX)) AS [StudyLeaveReason],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vPatInfo_ExportConsents) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    