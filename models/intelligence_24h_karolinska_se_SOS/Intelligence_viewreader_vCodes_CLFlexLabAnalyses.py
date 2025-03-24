
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Laboratoriets analyskatalog för beställning. Katalogen uppdateras kontinuerligt av laboratoriet utan versionshantering, vilket innebär att analyser tillkommer och tas bort.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisName': 'varchar(max)', 'AnalysisShortName': 'varchar(max)', 'CoOrder': 'varchar(max)', 'NoOfTubes': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)'},
    column_descriptions={'AnalysisID': "{'title_ui': None, 'description': 'Analyskod'}", 'AnalysisName': "{'title_ui': None, 'description': 'Analysnamn användes vid beställning av analys'}", 'AnalysisShortName': "{'title_ui': None, 'description': 'Kortnamn till etiketter'}", 'TubeID': "{'title_ui': None, 'description': 'Rörnummer Den typ av rör som analysen kräver'}", 'CoOrder': "{'title_ui': None, 'description': 'Sambeställning. Analyser med samma nummer kan sambeställas.'}", 'NoOfTubes': "{'title_ui': None, 'description': 'Rörantal'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AnalysisShortName] AS VARCHAR(MAX)) AS [AnalysisShortName],
		CAST([CoOrder] AS VARCHAR(MAX)) AS [CoOrder],
		CAST([NoOfTubes] AS VARCHAR(MAX)) AS [NoOfTubes],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TubeID] AS VARCHAR(MAX)) AS [TubeID] 
	FROM Intelligence.viewreader.vCodes_CLFlexLabAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    