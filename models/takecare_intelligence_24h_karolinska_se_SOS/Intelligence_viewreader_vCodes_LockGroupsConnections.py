
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kopplingar för vilka andra spärrgrupper som ska vara markerade i journalfilter när journal öppnas på en spärrgrupp.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ConnectedLockGroupID': 'varchar(max)', 'LockGroupID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'LockGroupID': "{'title_ui': None, 'description': 'Spärr ID'}", 'ConnectedLockGroupID': "{'title_ui': None, 'description': 'Kopplad spärr ID'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(ConnectedLockGroupID AS VARCHAR(MAX)) AS ConnectedLockGroupID,
		CAST(LockGroupID AS VARCHAR(MAX)) AS LockGroupID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_LockGroupsConnections) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    