
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="De vårdenheter som användaren är behörig till i rollen.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'RoleID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UserID': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'UserID': "{'title_ui': 'Personnr', 'description': 'Användarens personnummer'}", 'Version': "{'title_ui': None, 'description': 'Version av uppgifterna'}", 'RoleID': "{'title_ui': None, 'description': 'Vilken roll uppgifterna avser'}", 'CareUnitID': "{'title_ui': None, 'description': 'Vårdenhet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['CareUnitID', 'RoleID', 'UserID', 'Version']
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
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([RoleID] AS VARCHAR(MAX)) AS [RoleID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([UserID] AS VARCHAR(MAX)) AS [UserID],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vUsers_RolesCareUnitPrivs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    