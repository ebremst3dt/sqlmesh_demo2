
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Analyser som kan kopplas till en beställning (Immunologi IdaLab analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisName': 'varchar(max)', 'AnalysisShortName': 'varchar(max)', 'IsOrderable': 'varchar(max)', 'OrderTypeID': 'varchar(max)', 'PharmacyCode': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'AnalysisID': "{'title_ui': None, 'description': 'Analysid'}", 'AnalysisShortName': "{'title_ui': None, 'description': 'Analysens kortnamn'}", 'AnalysisName': "{'title_ui': None, 'description': 'Analysnamn'}", 'IsOrderable': "{'title_ui': None, 'description': 'Beställningsbar'}", 'PharmacyCode': "{'title_ui': None, 'description': 'Farmaciakod'}", 'OrderTypeID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['AnalysisID']
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
		CAST([AnalysisName] AS VARCHAR(MAX)) AS [AnalysisName],
		CAST([AnalysisShortName] AS VARCHAR(MAX)) AS [AnalysisShortName],
		CAST([IsOrderable] AS VARCHAR(MAX)) AS [IsOrderable],
		CAST([OrderTypeID] AS VARCHAR(MAX)) AS [OrderTypeID],
		CAST([PharmacyCode] AS VARCHAR(MAX)) AS [PharmacyCode],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_ImmIdaLabAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    