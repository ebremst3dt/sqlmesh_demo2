
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Aktiviteten markeras aldrig manuellt som utförd, utan står med som en påminnelse på Att-göra listan tills den sätts ut. T.ex. 'Prata med patienten'.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'EndDate': 'varchar(max)', 'EndTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'StartDate': 'varchar(max)', 'StartTime': 'varchar(max)', 'TaskID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När den första versionen av aktiviteten skapades'}", 'TaskID': "{'title_ui': None, 'description': 'Används när flera aktiviteter skapats på samma sekund.'}", 'TermID': "{'title_ui': None, 'description': 'Den term som definierar aktivitetens namn.'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'StartDate': "{'title_ui': 'Starttid', 'description': 'Datum då aktiviteten planeras att starta'}", 'StartTime': "{'title_ui': 'Starttid', 'description': 'Tid då aktiviteten planeras att starta'}", 'EndDate': "{'title_ui': 'Sluttid', 'description': 'Datum då aktiviteten planeras avslutas'}", 'EndTime': "{'title_ui': 'Sluttid', 'description': 'Tid då aktiviteten planeras avslutas'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['PatientID', 'TaskID', 'TermID', 'TimestampCreated', 'Version']
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
		CONVERT(varchar(max), [EndDate], 126) AS [EndDate],
		CONVERT(varchar(max), [EndTime], 126) AS [EndTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [StartDate], 126) AS [StartDate],
		CONVERT(varchar(max), [StartTime], 126) AS [StartTime],
		CAST([TaskID] AS VARCHAR(MAX)) AS [TaskID],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vTasks2_Continuous) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    