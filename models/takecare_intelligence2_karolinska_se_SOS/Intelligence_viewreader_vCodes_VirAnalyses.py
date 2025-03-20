
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Analyser som kan kopplas till en beställning (Virologlabb)",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Analysis': 'varchar(max)', 'AnalysisCode': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'IsOrderable': 'varchar(max)', 'IsPaperOrdered': 'varchar(max)', 'IsSolitaryOrderable': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'AnalysisID': "{'title_ui': None, 'description': 'Analysid'}", 'Analysis': "{'title_ui': None, 'description': 'Analysnamn'}", 'AnalysisCode': "{'title_ui': None, 'description': 'Unikt ID för analysen'}", 'IsOrderable': "{'title_ui': None, 'description': 'Beställningsbar'}", 'IsSolitaryOrderable': "{'title_ui': None, 'description': {'break': None}}", 'IsPaperOrdered': "{'title_ui': None, 'description': 'Beställs på papper'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily"
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(Analysis AS VARCHAR(MAX)) AS Analysis,
		CAST(AnalysisCode AS VARCHAR(MAX)) AS AnalysisCode,
		CAST(AnalysisID AS VARCHAR(MAX)) AS AnalysisID,
		CAST(IsOrderable AS VARCHAR(MAX)) AS IsOrderable,
		CAST(IsPaperOrdered AS VARCHAR(MAX)) AS IsPaperOrdered,
		CAST(IsSolitaryOrderable AS VARCHAR(MAX)) AS IsSolitaryOrderable,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_VirAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    