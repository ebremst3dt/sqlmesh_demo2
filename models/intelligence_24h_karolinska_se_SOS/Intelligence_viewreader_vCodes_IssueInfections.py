
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Hälsoproblem - Infektionsverktyget",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'IsSentAsOther': 'varchar(max)', 'IssueID': 'varchar(max)', 'IssueInfectionID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransmissionID': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'IssueInfectionID': "{'title_ui': 'Id', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'IssueID': "{'title_ui': 'Hälsoproblem Id', 'description': 'Hälsoproblemets Id'}", 'TransmissionID': "{'title_ui': 'Smittväg', 'description': 'Hälsoproblemets smittväg. 1=Samhällsförvärvad. 2=Vårdrelaterad'}", 'IsSentAsOther': '{\'title_ui\': \'Skicka som "Annat"\', \'description\': \'Om hälsoproblemet ska skickas som "Annat" till infektionsverktyget\'}', 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([IsSentAsOther] AS VARCHAR(MAX)) AS [IsSentAsOther],
		CAST([IssueID] AS VARCHAR(MAX)) AS [IssueID],
		CAST([IssueInfectionID] AS VARCHAR(MAX)) AS [IssueInfectionID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransmissionID] AS VARCHAR(MAX)) AS [TransmissionID],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_IssueInfections) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    