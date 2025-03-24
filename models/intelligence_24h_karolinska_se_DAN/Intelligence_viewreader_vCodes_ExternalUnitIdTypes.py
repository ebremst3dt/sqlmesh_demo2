
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Id-typer. Typer av externa IDn som identifierar tex. vårdenheter",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'IdTypeCode': 'varchar(max)', 'IdTypeID': 'varchar(max)', 'IdTypeIntelligenceName': 'varchar(max)', 'IdTypeName': 'varchar(max)', 'IdTypeRegistry': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'IdTypeID': "{'title_ui': None, 'description': None}", 'IdTypeCode': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': None, 'description': None}", 'IdTypeName': "{'title_ui': None, 'description': None}", 'IdTypeRegistry': "{'title_ui': None, 'description': None}", 'IdTypeIntelligenceName': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([IdTypeCode] AS VARCHAR(MAX)) AS [IdTypeCode],
		CAST([IdTypeID] AS VARCHAR(MAX)) AS [IdTypeID],
		CAST([IdTypeIntelligenceName] AS VARCHAR(MAX)) AS [IdTypeIntelligenceName],
		CAST([IdTypeName] AS VARCHAR(MAX)) AS [IdTypeName],
		CAST([IdTypeRegistry] AS VARCHAR(MAX)) AS [IdTypeRegistry],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_ExternalUnitIdTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    