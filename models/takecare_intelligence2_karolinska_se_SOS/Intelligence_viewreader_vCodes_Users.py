
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Alla registrerade användare i TakeCare-systemet.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'IsActive': 'varchar(max)', 'Name': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UserID': 'varchar(max)', 'UserName': 'varchar(max)'},
    column_descriptions={'UserID': "{'title_ui': 'Personnr', 'description': 'Användarens personnummer'}", 'Name': "{'title_ui': 'Förnamn+Efternamn', 'description': 'Användarens namn'}", 'UserName': "{'title_ui': 'Användarnamn', 'description': 'Användarens inloggningsnamn/signum'}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Den yrkesgrupp användaren tillhör'}", 'IsActive': "{'title_ui': 'Giltig t.o.m.', 'description': 'Om användarens konto är aktivt eller avstängt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(IsActive AS VARCHAR(MAX)) AS IsActive,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(ProfessionID AS VARCHAR(MAX)) AS ProfessionID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(UserID AS VARCHAR(MAX)) AS UserID,
		CAST(UserName AS VARCHAR(MAX)) AS UserName 
	FROM Intelligence.viewreader.vCodes_Users) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    