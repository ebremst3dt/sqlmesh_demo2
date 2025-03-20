
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Journalöppningar av användare via TakeCare-klienten""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CareUnitID': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampOpened': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UserID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Den journal som öppnades'}", 'TimestampOpened': "{'title_ui': None, 'description': 'Tidpunkt för journalöppning'}", 'UserID': "{'title_ui': None, 'description': 'Användaren som öppnade journalen'}", 'CareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet som användaren var inloggad på'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampOpened, 126) AS TimestampOpened,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(UserID AS VARCHAR(MAX)) AS UserID 
	FROM Intelligence.viewreader.vRecordLogs_Clients) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    