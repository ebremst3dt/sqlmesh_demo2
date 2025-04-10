
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Slutenvård - Företagkodsrelaterade storheter",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompanyCode': 'varchar(max)', 'CountyID': 'varchar(max)', 'EconomicalCatalogue': 'varchar(max)', 'InpatientCompanyID': 'varchar(max)', 'Name': 'varchar(max)', 'PlusGiro': 'varchar(max)', 'RESFolder': 'varchar(max)', 'RESUser': 'varchar(max)', 'SnodServer': 'varchar(max)', 'SnodUser': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'InpatientCompanyID': "{'title_ui': 'Id', 'description': None}", 'CompanyCode': "{'title_ui': 'FtgKod', 'description': 'Företagskod'}", 'Name': "{'title_ui': 'Företagsnamn', 'description': None}", 'PlusGiro': "{'title_ui': 'Plusgironummer', 'description': 'PlusGiro-konto'}", 'EconomicalCatalogue': "{'title_ui': 'Katalog för rapportering', 'description': 'Katalog för ekonomisk rapportering'}", 'RESUser': "{'title_ui': 'Användar ID mot RES', 'description': 'Användarnamn mot RES'}", 'RESFolder': "{'title_ui': 'Katalog för svarsfil från RES', 'description': None}", 'SnodUser': "{'title_ui': 'Snod-användarid', 'description': 'Användarnamn Snod (för överföring RES)'}", 'SnodServer': "{'title_ui': 'Snod-server', 'description': None}", 'CountyID': "{'title_ui': 'Länskod', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([CompanyCode] AS VARCHAR(MAX)) AS [CompanyCode],
		CAST([CountyID] AS VARCHAR(MAX)) AS [CountyID],
		CAST([EconomicalCatalogue] AS VARCHAR(MAX)) AS [EconomicalCatalogue],
		CAST([InpatientCompanyID] AS VARCHAR(MAX)) AS [InpatientCompanyID],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([PlusGiro] AS VARCHAR(MAX)) AS [PlusGiro],
		CAST([RESFolder] AS VARCHAR(MAX)) AS [RESFolder],
		CAST([RESUser] AS VARCHAR(MAX)) AS [RESUser],
		CAST([SnodServer] AS VARCHAR(MAX)) AS [SnodServer],
		CAST([SnodUser] AS VARCHAR(MAX)) AS [SnodUser],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_InpatientCompanies_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    