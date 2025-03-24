
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Rördefinitioner för CLab-analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'LIDNoExt': 'varchar(max)', 'LabelName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)', 'TubeName': 'varchar(max)'},
    column_descriptions={'TubeID': "{'title_ui': None, 'description': 'Rörnummer'}", 'TubeName': "{'title_ui': None, 'description': 'Rörnamn'}", 'LIDNoExt': "{'title_ui': None, 'description': 'Lidnr extension. Står efter lidnumret på etiketten.'}", 'LabelName': "{'title_ui': None, 'description': 'Etikettnamn för lab'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(LIDNoExt AS VARCHAR(MAX)) AS LIDNoExt,
		CAST(LabelName AS VARCHAR(MAX)) AS LabelName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TubeID AS VARCHAR(MAX)) AS TubeID,
		CAST(TubeName AS VARCHAR(MAX)) AS TubeName 
	FROM Intelligence.viewreader.vCodes_CLCLabTubes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    