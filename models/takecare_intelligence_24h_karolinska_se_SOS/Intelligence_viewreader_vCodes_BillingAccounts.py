
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassa - Kontoplan",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AccountID': 'varchar(max)', 'AccountNumber': 'varchar(max)', 'Counterpart': 'varchar(max)', 'CountyID': 'varchar(max)', 'Description': 'varchar(max)', 'SalesTaxClass': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'VATAccount': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'AccountID': "{'title_ui': 'ID', 'description': None}", 'Description': "{'title_ui': 'Beskrivning', 'description': 'Beskrivning av kontot'}", 'CountyID': "{'title_ui': 'Län', 'description': None}", 'AccountNumber': "{'title_ui': 'Kontonummer', 'description': None}", 'SalesTaxClass': "{'title_ui': 'Momsklass', 'description': None}", 'Counterpart': "{'title_ui': 'Motpart', 'description': None}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'VATAccount': "{'title_ui': 'Momskonto', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AccountID AS VARCHAR(MAX)) AS AccountID,
		CAST(AccountNumber AS VARCHAR(MAX)) AS AccountNumber,
		CAST(Counterpart AS VARCHAR(MAX)) AS Counterpart,
		CAST(CountyID AS VARCHAR(MAX)) AS CountyID,
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CAST(SalesTaxClass AS VARCHAR(MAX)) AS SalesTaxClass,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(VATAccount AS VARCHAR(MAX)) AS VATAccount,
		CONVERT(varchar(max), ValidFromDate, 126) AS ValidFromDate,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_BillingAccounts) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    