
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompanyID': 'varchar(max)', 'EconomicalCatalogue': 'varchar(max)', 'Name': 'varchar(max)', 'PlusGiro': 'varchar(max)', 'RESFolder': 'varchar(max)', 'RESUser': 'varchar(max)', 'SnodUser': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={},
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
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([EconomicalCatalogue] AS VARCHAR(MAX)) AS [EconomicalCatalogue],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([PlusGiro] AS VARCHAR(MAX)) AS [PlusGiro],
		CAST([RESFolder] AS VARCHAR(MAX)) AS [RESFolder],
		CAST([RESUser] AS VARCHAR(MAX)) AS [RESUser],
		CAST([SnodUser] AS VARCHAR(MAX)) AS [SnodUser],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_InpatientCompanies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    