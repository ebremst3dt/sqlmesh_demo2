
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""De alternativ som förvalt är intressanta att ange för detta hälsoproblem.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'IssueID': 'varchar(max)', 'Optional': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'IssueID': "{'title_ui': 'Id', 'description': None}", 'Optional': "{'title_ui': 'Alternativ', 'description': 'Alternativ som default ska användas för hälsoproblemet. 1=Smittväg. 2=KVÅ.Kod'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(IssueID AS VARCHAR(MAX)) AS IssueID,
		CAST(Optional AS VARCHAR(MAX)) AS Optional,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_IssueOptionals) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    