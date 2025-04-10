
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Profiler som en användare kan ha behörig till.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'IsAdmByUser': 'varchar(max)', 'ProfileID': 'varchar(max)', 'ProfileName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidToDate': 'varchar(max)'},
    column_descriptions={'ProfileID': "{'title_ui': 'Profilkod', 'description': 'Profilkod'}", 'ProfileName': "{'title_ui': 'Profilnamn', 'description': 'Profilens namn'}", 'IsAdmByUser': "{'title_ui': 'Användaradministrerad', 'description': 'Får administreras av normalanvändare'}", 'ValidToDate': "{'title_ui': 'Inaktiveringsdatum', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['ProfileID']
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
		CAST([IsAdmByUser] AS VARCHAR(MAX)) AS [IsAdmByUser],
		CAST([ProfileID] AS VARCHAR(MAX)) AS [ProfileID],
		CAST([ProfileName] AS VARCHAR(MAX)) AS [ProfileName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidToDate], 126) AS [ValidToDate] 
	FROM Intelligence.viewreader.vCodes_Profiles) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    