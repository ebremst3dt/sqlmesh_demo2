
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Betalningssätt i kassan. En uppsättning koder per företag.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BillingPaymentMethodCode': 'varchar(max)', 'BillingPaymentMethodID': 'varchar(max)', 'CompanyID': 'varchar(max)', 'ExternalCode': 'varchar(max)', 'IsHiddenInCounter': 'varchar(max)', 'IsMixed': 'varchar(max)', 'IsUptoFreecard': 'varchar(max)', 'IsValidForHealthCare': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'BillingPaymentMethodID': "{'title_ui': 'Id', 'description': None}", 'BillingPaymentMethodCode': "{'title_ui': 'Kod', 'description': None}", 'CompanyID': "{'title_ui': 'Kundkoder', 'description': None}", 'Name': "{'title_ui': 'Beskrivning', 'description': None}", 'IsValidForHealthCare': "{'title_ui': 'Giltig för försäljning hälsovård', 'description': None}", 'IsMixed': "{'title_ui': 'Mixad betalning', 'description': 'Betalsättet är både kontanter och kort'}", 'IsUptoFreecard': "{'title_ui': 'Upp till frikort', 'description': 'Betalsättet är giltigt för upp till frikort'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'ExternalCode': "{'title_ui': 'Extern Kod', 'description': {'break': [None, None, None, None]}}", 'IsHiddenInCounter': "{'title_ui': 'Dölj i kassan', 'description': 'Betalsättet döljs i kassan'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(BillingPaymentMethodCode AS VARCHAR(MAX)) AS BillingPaymentMethodCode,
		CAST(BillingPaymentMethodID AS VARCHAR(MAX)) AS BillingPaymentMethodID,
		CAST(CompanyID AS VARCHAR(MAX)) AS CompanyID,
		CAST(ExternalCode AS VARCHAR(MAX)) AS ExternalCode,
		CAST(IsHiddenInCounter AS VARCHAR(MAX)) AS IsHiddenInCounter,
		CAST(IsMixed AS VARCHAR(MAX)) AS IsMixed,
		CAST(IsUptoFreecard AS VARCHAR(MAX)) AS IsUptoFreecard,
		CAST(IsValidForHealthCare AS VARCHAR(MAX)) AS IsValidForHealthCare,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_BillingPaymentMethods_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    