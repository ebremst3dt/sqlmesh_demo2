
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassor. Kan kopplas till ekonomiska vårdenheter/kombikor i Codes_BillingKombikas genom kopplingstabellen Codes_BillingCounterKombikas. Kassor som saknar koppling till kombikor i Codes_BillingCounterKombikas, visas i TakeCare upp som kopplade till alla kombikor i vårdenhetsregistret.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompanyCode': 'varchar(max)', 'Counter': 'varchar(max)', 'CounterRow': 'varchar(max)', 'CustomerGroupCode': 'varchar(max)', 'Hospital': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'CounterRow': "{'title_ui': None, 'description': 'Används som id för kassan. Internt id som kan förändras.'}", 'Counter': "{'title_ui': 'Kassa', 'description': 'Kassakod'}", 'Name': "{'title_ui': 'Kassa', 'description': 'Kassanamn'}", 'Hospital': "{'title_ui': 'Inrättning', 'description': 'Inrättning'}", 'CompanyCode': "{'title_ui': 'Kundkod', 'description': 'Det som tidigare kallades företagskod, men egentligen var en kundkod'}", 'CustomerGroupCode': "{'title_ui': 'Kod', 'description': 'Kundgruppskod, är NULL när kundgrupp saknas'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CompanyCode] AS VARCHAR(MAX)) AS [CompanyCode],
		CAST([Counter] AS VARCHAR(MAX)) AS [Counter],
		CAST([CounterRow] AS VARCHAR(MAX)) AS [CounterRow],
		CAST([CustomerGroupCode] AS VARCHAR(MAX)) AS [CustomerGroupCode],
		CAST([Hospital] AS VARCHAR(MAX)) AS [Hospital],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_BillingCounters) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    