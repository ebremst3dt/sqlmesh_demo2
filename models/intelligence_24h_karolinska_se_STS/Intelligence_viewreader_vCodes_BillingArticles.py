
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassa - Artiklar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AccountID': 'varchar(max)', 'ArticleCode': 'varchar(max)', 'ArticleID': 'varchar(max)', 'IsOptionalPrice': 'varchar(max)', 'IsPreventiveHealthCare': 'varchar(max)', 'Keywords': 'varchar(max)', 'Name': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'ArticleID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': 'Namn', 'description': None}", 'ArticleCode': "{'title_ui': 'Artikelkod', 'description': None}", 'AccountID': "{'title_ui': 'Konto', 'description': None}", 'Keywords': "{'title_ui': 'Sökord', 'description': None}", 'IsOptionalPrice': "{'title_ui': 'Valfritt pris', 'description': None}", 'IsPreventiveHealthCare': "{'title_ui': 'Hälsovård', 'description': None}", 'ProfessionID': "{'title_ui': 'Yrke', 'description': 'Yrkeskategorin som utför hälsovård'}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': 'Första datum då data är giltigt'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AccountID] AS VARCHAR(MAX)) AS [AccountID],
		CAST([ArticleCode] AS VARCHAR(MAX)) AS [ArticleCode],
		CAST([ArticleID] AS VARCHAR(MAX)) AS [ArticleID],
		CAST([IsOptionalPrice] AS VARCHAR(MAX)) AS [IsOptionalPrice],
		CAST([IsPreventiveHealthCare] AS VARCHAR(MAX)) AS [IsPreventiveHealthCare],
		CAST([Keywords] AS VARCHAR(MAX)) AS [Keywords],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_BillingArticles) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    