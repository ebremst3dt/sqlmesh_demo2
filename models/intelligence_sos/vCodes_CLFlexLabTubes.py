
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Rördefinitioner för FlexLab-analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'LIDNoExt': 'varchar(max)', 'LabelName': 'varchar(max)', 'ShortName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)', 'TubeName': 'varchar(max)'},
    column_descriptions={'TubeID': "{'title_ui': None, 'description': 'Rörnummer'}", 'ShortName': "{'title_ui': None, 'description': 'Kortnamn'}", 'TubeName': "{'title_ui': None, 'description': 'Rörnamn'}", 'LIDNoExt': "{'title_ui': None, 'description': 'Lidnr extension. Står efter lidnummret på etiketten.'}", 'LabelName': "{'title_ui': None, 'description': 'Etikettnamn för lab (används ej för närvarande).'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['TubeID']
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
		CAST([LIDNoExt] AS VARCHAR(MAX)) AS [LIDNoExt],
		CAST([LabelName] AS VARCHAR(MAX)) AS [LabelName],
		CAST([ShortName] AS VARCHAR(MAX)) AS [ShortName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TubeID] AS VARCHAR(MAX)) AS [TubeID],
		CAST([TubeName] AS VARCHAR(MAX)) AS [TubeName] 
	FROM Intelligence.viewreader.vCodes_CLFlexLabTubes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    