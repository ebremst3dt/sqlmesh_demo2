
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Villkor för numeriska termer. Villkor som används vid validering av termer innehållande numeriska värden",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ConditionID': 'varchar(max)', 'Description': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UseOperand1': 'varchar(max)', 'UseOperand2': 'varchar(max)'},
    column_descriptions={'ConditionID': "{'title_ui': None, 'description': None}", 'Description': "{'title_ui': None, 'description': None}", 'UseOperand1': "{'title_ui': None, 'description': None}", 'UseOperand2': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['ConditionID']
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
		CAST([ConditionID] AS VARCHAR(MAX)) AS [ConditionID],
		CAST([Description] AS VARCHAR(MAX)) AS [Description],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([UseOperand1] AS VARCHAR(MAX)) AS [UseOperand1],
		CAST([UseOperand2] AS VARCHAR(MAX)) AS [UseOperand2] 
	FROM Intelligence.viewreader.vCodes_TermConditions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    