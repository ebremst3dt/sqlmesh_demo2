
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Journalöppningar från externa system via Xchange-servern",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ExternalUserHSAID': 'varchar(max)', 'ExternalUserID': 'varchar(max)', 'PatientID': 'varchar(max)', 'SystemUserName': 'varchar(max)', 'TimestampOpened': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Den journal som öppnades'}", 'TimestampOpened': "{'title_ui': None, 'description': 'Tidpunkt för journalöppning'}", 'SystemUserName': "{'title_ui': None, 'description': 'Det externa systemets användarnamn i TakeCare'}", 'ExternalUserID': "{'title_ui': None, 'description': 'Ev. extern användare som öppnade journalen'}", 'ExternalUserHSAID': "{'title_ui': None, 'description': 'HSAID för ev. extern användare som öppnade journalen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(ExternalUserHSAID AS VARCHAR(MAX)) AS ExternalUserHSAID,
		CAST(ExternalUserID AS VARCHAR(MAX)) AS ExternalUserID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(SystemUserName AS VARCHAR(MAX)) AS SystemUserName,
		CONVERT(varchar(max), TimestampOpened, 126) AS TimestampOpened,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vRecordLogs_ExternalSystems) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    