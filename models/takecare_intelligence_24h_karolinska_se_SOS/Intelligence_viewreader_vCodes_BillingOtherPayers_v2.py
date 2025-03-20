
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Annan betalare vid kreditbesök i kassan. En uppsättning koder per företag.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'BillingOtherPayerCode': 'varchar(max)', 'BillingOtherPayerID': 'varchar(max)', 'CompanyID': 'varchar(max)', 'Counterpart': 'varchar(max)', 'EXID': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'BillingOtherPayerID': "{'title_ui': 'Id', 'description': None}", 'BillingOtherPayerCode': "{'title_ui': 'Kod', 'description': None}", 'CompanyID': "{'title_ui': 'Kundkoder', 'description': None}", 'Name': "{'title_ui': 'Beskrivning', 'description': None}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': 'Första datum då data är giltigt'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'EXID': "{'title_ui': 'Extern enhet', 'description': 'Extern enhets koppling för adress data etc.'}", 'Counterpart': "{'title_ui': 'Motpart', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(BillingOtherPayerCode AS VARCHAR(MAX)) AS BillingOtherPayerCode,
		CAST(BillingOtherPayerID AS VARCHAR(MAX)) AS BillingOtherPayerID,
		CAST(CompanyID AS VARCHAR(MAX)) AS CompanyID,
		CAST(Counterpart AS VARCHAR(MAX)) AS Counterpart,
		CAST(EXID AS VARCHAR(MAX)) AS EXID,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidFromDate, 126) AS ValidFromDate,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_BillingOtherPayers_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    