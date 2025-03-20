
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Aktiviteten utförs var n:te dag, en till flera gånger varje dag.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'EndDate': 'varchar(max)', 'EndTime': 'varchar(max)', 'EveryNDays': 'varchar(max)', 'PatientID': 'varchar(max)', 'StartDate': 'varchar(max)', 'StartTime': 'varchar(max)', 'TaskID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När den första versionen av aktiviteten skapades'}", 'TaskID': "{'title_ui': None, 'description': 'Används när flera aktiviteter skapats på samma sekund.'}", 'TermID': "{'title_ui': None, 'description': 'Den term som definierar aktivitetens namn.'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'StartDate': "{'title_ui': 'Starttid', 'description': 'Datum då aktiviteten planeras att starta'}", 'StartTime': "{'title_ui': 'Starttid', 'description': 'Tid då aktiviteten planeras att starta'}", 'EndDate': "{'title_ui': 'Sluttid', 'description': 'Datum då aktiviteten planeras avslutas'}", 'EndTime': "{'title_ui': 'Sluttid', 'description': 'Tid då aktiviteten planeras avslutas'}", 'EveryNDays': "{'title_ui': 'Upprepa var ... :e dag', 'description': 'T.ex. 3 för var tredje dag.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CONVERT(varchar(max), EndDate, 126) AS EndDate,
		CONVERT(varchar(max), EndTime, 126) AS EndTime,
		CAST(EveryNDays AS VARCHAR(MAX)) AS EveryNDays,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), StartDate, 126) AS StartDate,
		CONVERT(varchar(max), StartTime, 126) AS StartTime,
		CAST(TaskID AS VARCHAR(MAX)) AS TaskID,
		CAST(TermID AS VARCHAR(MAX)) AS TermID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vTasks2_ScheduleDay) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    