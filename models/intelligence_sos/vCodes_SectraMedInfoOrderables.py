
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Vilka kompletterande uppgifter (tidigare medicinsk information) som hör till varje beställningsspec. (Röntgen Sectra analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'IsRequired': 'varchar(max)', 'MedInfoID': 'varchar(max)', 'OrderableID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderableID': "{'title_ui': None, 'description': 'Beställningsspec-id'}", 'MedInfoID': "{'title_ui': None, 'description': 'Kompl. uppgift-id'}", 'IsRequired': "{'title_ui': None, 'description': 'Obligatorisk'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['MedInfoID', 'OrderableID']
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
		CAST([IsRequired] AS VARCHAR(MAX)) AS [IsRequired],
		CAST([MedInfoID] AS VARCHAR(MAX)) AS [MedInfoID],
		CAST([OrderableID] AS VARCHAR(MAX)) AS [OrderableID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_SectraMedInfoOrderables) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    