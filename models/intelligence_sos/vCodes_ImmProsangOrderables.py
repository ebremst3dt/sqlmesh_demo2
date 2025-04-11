
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Beställningsspec. En specifikation består av en unik kombination av analys och materiel. (Immunologi Prosang analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'MaterielID': 'varchar(max)', 'NoOfLabels': 'varchar(max)', 'OrderableID': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderableID': "{'title_ui': None, 'description': 'Beställningsspecifikationsid'}", 'SpecimenID': "{'title_ui': None, 'description': 'Provmaterialid'}", 'AnalysisID': "{'title_ui': None, 'description': 'Analysid'}", 'MaterielID': "{'title_ui': None, 'description': None}", 'NoOfLabels': "{'title_ui': None, 'description': 'Antal etiketter'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['OrderableID']
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
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([MaterielID] AS VARCHAR(MAX)) AS [MaterielID],
		CAST([NoOfLabels] AS VARCHAR(MAX)) AS [NoOfLabels],
		CAST([OrderableID] AS VARCHAR(MAX)) AS [OrderableID],
		CAST([SpecimenID] AS VARCHAR(MAX)) AS [SpecimenID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_ImmProsangOrderables) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    