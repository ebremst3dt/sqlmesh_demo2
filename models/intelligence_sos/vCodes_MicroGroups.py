
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Grupper av paket. En grupp kan ingå i en annan grupp (Mikrolabb analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CategoryID': 'varchar(max)', 'GroupID': 'varchar(max)', 'GroupName': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'SuperiorGroupID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'GroupID': "{'title_ui': None, 'description': 'Grupp-id'}", 'GroupName': "{'title_ui': None, 'description': 'Gruppnamn'}", 'CategoryID': "{'title_ui': None, 'description': 'Vilken kategori gruppen tillhör'}", 'SuperiorGroupID': "{'title_ui': None, 'description': 'Vilken överordnad grupp som gruppen tillhör'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['GroupID', 'OrderRegistryFileName']
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
		CAST([CategoryID] AS VARCHAR(MAX)) AS [CategoryID],
		CAST([GroupID] AS VARCHAR(MAX)) AS [GroupID],
		CAST([GroupName] AS VARCHAR(MAX)) AS [GroupName],
		CAST([OrderRegistryFileName] AS VARCHAR(MAX)) AS [OrderRegistryFileName],
		CAST([SuperiorGroupID] AS VARCHAR(MAX)) AS [SuperiorGroupID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_MicroGroups) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    