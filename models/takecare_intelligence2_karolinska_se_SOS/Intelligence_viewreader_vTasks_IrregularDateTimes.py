
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Planerade datum/tid för oregelbundna aktiviteter""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Date': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'TaskID': 'varchar(max)', 'TermID': 'varchar(max)', 'Time': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När den första versionen av aktiviteten skapades'}", 'TaskID': "{'title_ui': None, 'description': 'Används när flera aktiviteter skapats på samma sekund.'}", 'TermID': "{'title_ui': None, 'description': 'Den term som definierar aktivitetens namn.'}", 'Row': "{'title_ui': None, 'description': 'Löpnummer för planerad datum/tid'}", 'Date': "{'title_ui': 'Planerad datum', 'description': 'Datum för detta planerade tillfälle'}", 'Time': "{'title_ui': 'Planerad tid', 'description': 'Tid för detta planerade tillfälle'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), Date, 126) AS Date,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(TaskID AS VARCHAR(MAX)) AS TaskID,
		CAST(TermID AS VARCHAR(MAX)) AS TermID,
		CONVERT(varchar(max), Time, 126) AS Time,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vTasks_IrregularDateTimes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    