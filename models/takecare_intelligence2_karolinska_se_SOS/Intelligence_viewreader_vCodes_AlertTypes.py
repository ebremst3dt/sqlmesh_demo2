
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Olika typer av varningar för medicinska patientuppgifter""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AlertTypeID': 'varchar(max)', 'Description': 'varchar(max)', 'LongName': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'AlertTypeID': "{'title_ui': None, 'description': None}", 'Name': "{'title_ui': None, 'description': 'Varningstypens namn/beteckning'}", 'LongName': "{'title_ui': None, 'description': 'Längre namn för varningstypen'}", 'Description': "{'title_ui': None, 'description': 'Beskrivning'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AlertTypeID AS VARCHAR(MAX)) AS AlertTypeID,
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CAST(LongName AS VARCHAR(MAX)) AS LongName,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_AlertTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    