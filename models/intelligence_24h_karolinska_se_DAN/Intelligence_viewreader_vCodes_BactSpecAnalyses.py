
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Vilka analyser som ingår i vilket provmaterial (Baktlabb)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'BelongsTo': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'SpecimenID': "{'title_ui': None, 'description': 'Promaterial-id'}", 'AnalysisID': "{'title_ui': None, 'description': 'Analys-id'}", 'BelongsTo': "{'title_ui': None, 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BelongsTo] AS VARCHAR(MAX)) AS [BelongsTo],
		CAST([SpecimenID] AS VARCHAR(MAX)) AS [SpecimenID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_BactSpecAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    