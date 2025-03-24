
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Register över de vårdgivare som finns inlagda i systemet (kallades tidigare domäner). Användare hos en vårdgivare har inte automatiskt tillgång till journaldata från en annan vårdgivare.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DomainID': 'varchar(max)', 'HSAID': 'varchar(max)', 'HasCoherentRecords': 'varchar(max)', 'Name': 'varchar(max)', 'OrgNo': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'DomainID': "{'title_ui': 'Identitet', 'description': None}", 'Name': "{'title_ui': 'Vårdgivarnamn', 'description': 'Namn på vårdgivaren'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då vårdgivaren är giltig'}", 'HasCoherentRecords': "{'title_ui': 'Sammanhållen journal', 'description': 'Om vårdgivaren tillämpar sammanhållen journalföring'}", 'HSAID': "{'title_ui': 'HSA-id', 'description': 'HSA-id för vårdgivare'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DomainID AS VARCHAR(MAX)) AS DomainID,
		CAST(HSAID AS VARCHAR(MAX)) AS HSAID,
		CAST(HasCoherentRecords AS VARCHAR(MAX)) AS HasCoherentRecords,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(OrgNo AS VARCHAR(MAX)) AS OrgNo,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_Domains) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    