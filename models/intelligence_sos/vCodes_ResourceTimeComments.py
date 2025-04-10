
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read

    
@model(
    description="Man kan ange en kommentar på en viss resurs tid, t.ex 'Lunch'.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'Comment': 'varchar(max)', 'CommentDatetime': 'varchar(max)', 'ResourceID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'ResourceID': "{'title_ui': 'Resurs', 'description': None}", 'CommentDatetime': "{'title_ui': 'Tidpunkt', 'description': 'Tid som kommentaren gäller för.'}", 'SavedByUserID': "{'title_ui': 'Sparad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CONVERT(varchar(max), [CommentDatetime], 126) AS [CommentDatetime],
		CAST([ResourceID] AS VARCHAR(MAX)) AS [ResourceID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_ResourceTimeComments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    