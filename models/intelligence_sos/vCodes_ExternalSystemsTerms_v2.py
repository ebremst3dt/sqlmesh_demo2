
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Termmappning för externa tjänster och register",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ExternalSystemID': 'varchar(max)', 'ExternalTermName': 'varchar(max)', 'InternalTermID': 'varchar(max)', 'IsActive': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'ExternalSystemID': "{'title_ui': None, 'description': 'TakeCares interna id för det externa systemet'}", 'ExternalTermName': "{'title_ui': 'Externt uttryck & Extern grupp', 'description': 'Namn på det externa systemets term. Innehåller ibland även namn på det externa systemets grupp och lagras då som gruppnamn:termnamn.'}", 'InternalTermID': "{'title_ui': 'Term id', 'description': 'En TakeCare-term som motsvarar det externa systemets term'}", 'IsActive': "{'title_ui': None, 'description': 'Term aktiverad via bockruta i termmappningslistan'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['ExternalSystemID', 'ExternalTermName', 'InternalTermID']
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
		CAST([ExternalSystemID] AS VARCHAR(MAX)) AS [ExternalSystemID],
		CAST([ExternalTermName] AS VARCHAR(MAX)) AS [ExternalTermName],
		CAST([InternalTermID] AS VARCHAR(MAX)) AS [InternalTermID],
		CAST([IsActive] AS VARCHAR(MAX)) AS [IsActive],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_ExternalSystemsTerms_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    