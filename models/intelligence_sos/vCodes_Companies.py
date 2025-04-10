
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BICCode': 'varchar(max)', 'BankGiro': 'varchar(max)', 'CompanyID': 'varchar(max)', 'County': 'varchar(max)', 'CountyID': 'varchar(max)', 'CreditDays': 'varchar(max)', 'EconomicalCatalogue': 'varchar(max)', 'IBAN': 'varchar(max)', 'InvoiceNoSeriesEnd': 'varchar(max)', 'InvoiceNoSeriesStart': 'varchar(max)', 'InvoicingCharge': 'varchar(max)', 'Logotype': 'varchar(max)', 'Name': 'varchar(max)', 'OrgNo': 'varchar(max)', 'PGOrBG': 'varchar(max)', 'PhoneNo': 'varchar(max)', 'PlusGiro': 'varchar(max)', 'PlusGiroContract': 'varchar(max)', 'PlusGiroRecipient': 'varchar(max)', 'PostalAddress': 'varchar(max)', 'RESFolder': 'varchar(max)', 'RESUser': 'varchar(max)', 'ReminderCharge': 'varchar(max)', 'SnodUser': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
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
		CAST([BICCode] AS VARCHAR(MAX)) AS [BICCode],
		CAST([BankGiro] AS VARCHAR(MAX)) AS [BankGiro],
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([County] AS VARCHAR(MAX)) AS [County],
		CAST([CountyID] AS VARCHAR(MAX)) AS [CountyID],
		CAST([CreditDays] AS VARCHAR(MAX)) AS [CreditDays],
		CAST([EconomicalCatalogue] AS VARCHAR(MAX)) AS [EconomicalCatalogue],
		CAST([IBAN] AS VARCHAR(MAX)) AS [IBAN],
		CAST([InvoiceNoSeriesEnd] AS VARCHAR(MAX)) AS [InvoiceNoSeriesEnd],
		CAST([InvoiceNoSeriesStart] AS VARCHAR(MAX)) AS [InvoiceNoSeriesStart],
		CAST([InvoicingCharge] AS VARCHAR(MAX)) AS [InvoicingCharge],
		CAST([Logotype] AS VARCHAR(MAX)) AS [Logotype],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([OrgNo] AS VARCHAR(MAX)) AS [OrgNo],
		CAST([PGOrBG] AS VARCHAR(MAX)) AS [PGOrBG],
		CAST([PhoneNo] AS VARCHAR(MAX)) AS [PhoneNo],
		CAST([PlusGiro] AS VARCHAR(MAX)) AS [PlusGiro],
		CAST([PlusGiroContract] AS VARCHAR(MAX)) AS [PlusGiroContract],
		CAST([PlusGiroRecipient] AS VARCHAR(MAX)) AS [PlusGiroRecipient],
		CAST([PostalAddress] AS VARCHAR(MAX)) AS [PostalAddress],
		CAST([RESFolder] AS VARCHAR(MAX)) AS [RESFolder],
		CAST([RESUser] AS VARCHAR(MAX)) AS [RESUser],
		CAST([ReminderCharge] AS VARCHAR(MAX)) AS [ReminderCharge],
		CAST([SnodUser] AS VARCHAR(MAX)) AS [SnodUser],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_Companies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    