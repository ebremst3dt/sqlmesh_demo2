
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kompletterar taxorna med data för avtalskunder/storkunder. En uppsättning koder per företag.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BillingInvoiceRateCode': 'varchar(max)', 'BillingInvoiceRateID': 'varchar(max)', 'CompanyID': 'varchar(max)', 'Name': 'varchar(max)', 'Rate': 'varchar(max)', 'RateNonDoctor': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'BillingInvoiceRateID': "{'title_ui': 'Id', 'description': None}", 'BillingInvoiceRateCode': "{'title_ui': 'Kod', 'description': None}", 'CompanyID': "{'title_ui': 'Kundkoder', 'description': None}", 'Name': "{'title_ui': 'Beskrivning', 'description': None}", 'Rate': "{'title_ui': 'Pris läkare', 'description': None}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': 'Första datum då data är giltigt'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'RateNonDoctor': "{'title_ui': 'Pris icke läkare', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BillingInvoiceRateCode] AS VARCHAR(MAX)) AS [BillingInvoiceRateCode],
		CAST([BillingInvoiceRateID] AS VARCHAR(MAX)) AS [BillingInvoiceRateID],
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([Rate] AS VARCHAR(MAX)) AS [Rate],
		CAST([RateNonDoctor] AS VARCHAR(MAX)) AS [RateNonDoctor],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_BillingInvoiceRates_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    