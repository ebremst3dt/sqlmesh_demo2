
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Information om användarnas roller.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'EmploymentCareUnitGroupID': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'RoleID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UserID': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'UserID': "{'title_ui': 'Personnr', 'description': 'Användarens personnummer'}", 'Version': "{'title_ui': None, 'description': 'Version av uppgifterna'}", 'RoleID': "{'title_ui': None, 'description': 'Vilken roll uppgifterna avser'}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Yrkesgruppskod'}", 'EmploymentCareUnitGroupID': "{'title_ui': 'Anställd på vårdenhetsgrupp', 'description': None}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(EmploymentCareUnitGroupID AS VARCHAR(MAX)) AS EmploymentCareUnitGroupID,
		CAST(ProfessionID AS VARCHAR(MAX)) AS ProfessionID,
		CAST(RoleID AS VARCHAR(MAX)) AS RoleID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(UserID AS VARCHAR(MAX)) AS UserID,
		CONVERT(varchar(max), ValidFromDate, 126) AS ValidFromDate,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vUsers_Roles) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    