
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Metoder som kan kopplas till en beställning (Generell analyskatalog för Fysiologi och Röntgen)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MethodID': 'varchar(max)', 'MethodName': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'MethodID': "{'title_ui': None, 'description': 'Metod-id'}", 'MethodName': "{'title_ui': None, 'description': 'Metodnamn'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([MethodID] AS VARCHAR(MAX)) AS [MethodID],
		CAST([MethodName] AS VARCHAR(MAX)) AS [MethodName],
		CAST([OrderRegistryFileName] AS VARCHAR(MAX)) AS [OrderRegistryFileName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_PhysRadGeneralMethods) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    