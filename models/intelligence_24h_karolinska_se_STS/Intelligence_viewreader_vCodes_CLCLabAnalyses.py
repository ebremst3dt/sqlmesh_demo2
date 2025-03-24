
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Analyser som kan kopplas till en beställning (CLab), nedlagd, användes till 2004",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisName': 'varchar(max)', 'CoOrder': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'IsOKToOrder': 'varchar(max)', 'IsPaperOrdered': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)', 'Unit': 'varchar(max)'},
    column_descriptions={'AnalysisID': "{'title_ui': None, 'description': 'Analyskod'}", 'AnalysisName': "{'title_ui': None, 'description': 'Analysnamn. Används vid beställning av analys'}", 'Unit': "{'title_ui': 'Enhet', 'description': None}", 'IsOKToOrder': "{'title_ui': None, 'description': 'Beställningsbar. Om 1 syns analysen i beställningbilden.'}", 'TubeID': "{'title_ui': None, 'description': 'Rörnummer. Den typ av rör som analysen kräver'}", 'IsPaperOrdered': "{'title_ui': None, 'description': 'Beställs på papper'}", 'IsEmergency': "{'title_ui': None, 'description': 'Akut'}", 'CoOrder': "{'title_ui': None, 'description': 'Sambeställning. Analyser med samma nummer kan sambeställas.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([AnalysisName] AS VARCHAR(MAX)) AS [AnalysisName],
		CAST([CoOrder] AS VARCHAR(MAX)) AS [CoOrder],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([IsOKToOrder] AS VARCHAR(MAX)) AS [IsOKToOrder],
		CAST([IsPaperOrdered] AS VARCHAR(MAX)) AS [IsPaperOrdered],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TubeID] AS VARCHAR(MAX)) AS [TubeID],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit] 
	FROM Intelligence.viewreader.vCodes_CLCLabAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    