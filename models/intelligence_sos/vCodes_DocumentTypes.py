
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Dokumenttyper i Intelligence. Flera dokumenttyps-id kan höra till en och samma tabellgrupp.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentTypeID': 'varchar(max)', 'DocumentTypeName': 'varchar(max)', 'TableName': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'DocumentTypeID': "{'title_ui': None, 'description': 'Id för dokumenttyp'}", 'DocumentTypeName': "{'title_ui': None, 'description': 'Namn på dokumenttypen'}", 'TableName': "{'title_ui': None, 'description': 'Namn på tabellgruppen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DocumentTypeID] AS VARCHAR(MAX)) AS [DocumentTypeID],
		CAST([DocumentTypeName] AS VARCHAR(MAX)) AS [DocumentTypeName],
		CAST([TableName] AS VARCHAR(MAX)) AS [TableName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_DocumentTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    