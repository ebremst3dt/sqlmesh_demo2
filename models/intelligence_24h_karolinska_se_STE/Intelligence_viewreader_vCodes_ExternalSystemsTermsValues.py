
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Värdemappning för externa tjänster och register",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ExternalSystemID': 'varchar(max)', 'ExternalTermName': 'varchar(max)', 'ExternalTermValue': 'varchar(max)', 'InternalTermID': 'varchar(max)', 'InternalTermValue': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'ExternalSystemID': "{'title_ui': None, 'description': 'TakeCares interna id för det externa systemet'}", 'ExternalTermName': "{'title_ui': 'Externt uttryck & Extern grupp', 'description': 'Namn på det externa systemets term. Innehåller ibland även namn på det externa systemets grupp och lagras då som gruppnamn:termnamn.'}", 'InternalTermID': "{'title_ui': 'Term id', 'description': 'En TakeCare-term som motsvarar det externa systemets term'}", 'Row': "{'title_ui': None, 'description': 'Unik rad nyckel'}", 'InternalTermValue': "{'title_ui': 'Internt värde', 'description': 'Värde för TakeCare-termen som motsvarar värde för den externa termen'}", 'ExternalTermValue': "{'title_ui': 'Externt värde', 'description': 'Värde för den externa termen som motsvarar värde för TakeCare-termen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
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
		CAST([ExternalTermValue] AS VARCHAR(MAX)) AS [ExternalTermValue],
		CAST([InternalTermID] AS VARCHAR(MAX)) AS [InternalTermID],
		CAST([InternalTermValue] AS VARCHAR(MAX)) AS [InternalTermValue],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_ExternalSystemsTermsValues) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    