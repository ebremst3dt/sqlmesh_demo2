
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Bedömningsstatus konsultationsärende.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AltNameSingular': 'varchar(max)', 'ConsultationCaseStatusID': 'varchar(max)', 'NamePlural': 'varchar(max)', 'NameSingular': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'ConsultationCaseStatusID': "{'title_ui': None, 'description': None}", 'NameSingular': "{'title_ui': None, 'description': None}", 'NamePlural': "{'title_ui': None, 'description': None}", 'AltNameSingular': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AltNameSingular] AS VARCHAR(MAX)) AS [AltNameSingular],
		CAST([ConsultationCaseStatusID] AS VARCHAR(MAX)) AS [ConsultationCaseStatusID],
		CAST([NamePlural] AS VARCHAR(MAX)) AS [NamePlural],
		CAST([NameSingular] AS VARCHAR(MAX)) AS [NameSingular],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_ConsultationCaseStatuses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    