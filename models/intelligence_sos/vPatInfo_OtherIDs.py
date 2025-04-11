
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Journaler kan kopplas ihop i TakeCare. Exempelvis kopplas en tidigare okänd persons journal ihop med en känd person, när dennes personnummer blir känt. Dessa kopplade id:n pekar ut andra journaler för samma patient. Kopplingen är inte implicit symmetrisk eller kommutativ i datat.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ConnectedDatetime': 'varchar(max)', 'ConnectedPatientID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ConnectedPatientID': "{'title_ui': 'Kopplat id', 'description': 'Det ihopkopplade id:t'}", 'ConnectedDatetime': "{'title_ui': None, 'description': 'Tid då ihopkoppling registrerats'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som registrerat kopplingen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), [ConnectedDatetime], 126) AS [ConnectedDatetime],
		CAST([ConnectedPatientID] AS VARCHAR(MAX)) AS [ConnectedPatientID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vPatInfo_OtherIDs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    