
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Koppling mellan substanser/preparat och farmlab-analyser. (MultiLabb analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'OrderableID': 'varchar(max)', 'SubstancePreparationID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'OrderableID': "{'title_ui': None, 'description': 'Beställningsspec-id'}", 'SubstancePreparationID': "{'title_ui': None, 'description': 'Id för att koppla ihop en farmanalys med substans/preparat'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['OrderableID', 'OrderRegistryFileName', 'SubstancePreparationID']
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
		CAST([OrderRegistryFileName] AS VARCHAR(MAX)) AS [OrderRegistryFileName],
		CAST([OrderableID] AS VARCHAR(MAX)) AS [OrderableID],
		CAST([SubstancePreparationID] AS VARCHAR(MAX)) AS [SubstancePreparationID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_MultiLabOrderablesPharmPreps) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    