
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Aktiviteten utförs enligt schema vissa veckodagar, en gång per dag.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'EndDate': 'varchar(max)', 'EndTime': 'varchar(max)', 'EveryNWeeks': 'varchar(max)', 'IsPerformedOnFridays': 'varchar(max)', 'IsPerformedOnMondays': 'varchar(max)', 'IsPerformedOnSaturdays': 'varchar(max)', 'IsPerformedOnSundays': 'varchar(max)', 'IsPerformedOnThursdays': 'varchar(max)', 'IsPerformedOnTuesdays': 'varchar(max)', 'IsPerformedOnWednesdays': 'varchar(max)', 'PatientID': 'varchar(max)', 'StartDate': 'varchar(max)', 'StartTime': 'varchar(max)', 'TaskID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När den första versionen av aktiviteten skapades'}", 'TaskID': "{'title_ui': None, 'description': 'Används när flera aktiviteter skapats på samma sekund.'}", 'TermID': "{'title_ui': None, 'description': 'Den term som definierar aktivitetens namn.'}", 'StartDate': "{'title_ui': 'Starttid', 'description': 'Datum då aktiviteten planeras att starta'}", 'StartTime': "{'title_ui': 'Starttid', 'description': 'Tid då aktiviteten planeras att starta'}", 'EndDate': "{'title_ui': 'Sluttid', 'description': 'Datum då aktiviteten planeras avslutas'}", 'EndTime': "{'title_ui': 'Sluttid', 'description': 'Tid då aktiviteten planeras avslutas'}", 'EveryNWeeks': "{'title_ui': 'Upprepa var ... :e vecka', 'description': 'T.ex. 4 för var fjärde vecka.'}", 'IsPerformedOnMondays': "{'title_ui': 'Må', 'description': 'Om aktiviteten ska utföras på måndagar'}", 'IsPerformedOnTuesdays': "{'title_ui': 'Ti', 'description': 'Om aktiviteten ska utföras på tisdagar'}", 'IsPerformedOnWednesdays': "{'title_ui': 'On', 'description': 'Om aktiviteten ska utföras på onsdagar'}", 'IsPerformedOnThursdays': "{'title_ui': 'To', 'description': 'Om aktiviteten ska utföras på torsdagar'}", 'IsPerformedOnFridays': "{'title_ui': 'Fr', 'description': 'Om aktiviteten ska utföras på fredagar'}", 'IsPerformedOnSaturdays': "{'title_ui': 'Lö', 'description': 'Om aktiviteten ska utföras på lördagar'}", 'IsPerformedOnSundays': "{'title_ui': 'Sö', 'description': 'Om aktiviteten ska utföras på söndagar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['PatientID', 'TaskID', 'TermID', 'TimestampCreated']
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
		CAST([EveryNWeeks] AS VARCHAR(MAX)) AS [EveryNWeeks],
		CAST([IsPerformedOnFridays] AS VARCHAR(MAX)) AS [IsPerformedOnFridays],
		CAST([IsPerformedOnMondays] AS VARCHAR(MAX)) AS [IsPerformedOnMondays],
		CAST([IsPerformedOnSaturdays] AS VARCHAR(MAX)) AS [IsPerformedOnSaturdays],
		CAST([IsPerformedOnSundays] AS VARCHAR(MAX)) AS [IsPerformedOnSundays],
		CAST([IsPerformedOnThursdays] AS VARCHAR(MAX)) AS [IsPerformedOnThursdays],
		CAST([IsPerformedOnTuesdays] AS VARCHAR(MAX)) AS [IsPerformedOnTuesdays],
		CAST([IsPerformedOnWednesdays] AS VARCHAR(MAX)) AS [IsPerformedOnWednesdays],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [StartDate], 126) AS [StartDate],
		CONVERT(varchar(max), [StartTime], 126) AS [StartTime],
		CAST([TaskID] AS VARCHAR(MAX)) AS [TaskID],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vTasks_ScheduleWeek) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    