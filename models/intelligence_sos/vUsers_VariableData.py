
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Ej versionshanterat data om de registrerade användarna.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'PrescriberCode': 'varchar(max)', 'PrescriberTitleID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UserID': 'varchar(max)'},
    column_descriptions={'UserID': "{'title_ui': 'Personnr', 'description': 'Användarens personnummer'}", 'PrescriberCode': "{'title_ui': 'Förskrivarkod', 'description': None}", 'PrescriberTitleID': "{'title_ui': 'Förskrivartyp', 'description': 'Apotekskoden för förskrivartypen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([PrescriberCode] AS VARCHAR(MAX)) AS [PrescriberCode],
		CAST([PrescriberTitleID] AS VARCHAR(MAX)) AS [PrescriberTitleID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([UserID] AS VARCHAR(MAX)) AS [UserID] 
	FROM Intelligence.viewreader.vUsers_VariableData) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    