
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassa - Avgiftsreducering",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'BillingFeeReductionID': 'varchar(max)', 'BillingRateCode': 'varchar(max)', 'EmergencyWard': 'varchar(max)', 'MaximumAge': 'varchar(max)', 'MinimumAge': 'varchar(max)', 'MultiplyBy': 'varchar(max)', 'Name': 'varchar(max)', 'SubtractFrom': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'BillingFeeReductionID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m', 'description': None}", 'MinimumAge': "{'title_ui': 'Ålder fr.o.m', 'description': None}", 'MaximumAge': "{'title_ui': 'Ålder upp till', 'description': None}", 'EmergencyWard': "{'title_ui': 'Akutenhet', 'description': '1=Ej Akutenhet, 2=Akutenhet'}", 'BillingRateCode': "{'title_ui': 'Taxa', 'description': None}", 'MultiplyBy': "{'title_ui': 'Multiplicera taxa med', 'description': None}", 'SubtractFrom': "{'title_ui': 'Subtrahera från taxa', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(BillingFeeReductionID AS VARCHAR(MAX)) AS BillingFeeReductionID,
		CAST(BillingRateCode AS VARCHAR(MAX)) AS BillingRateCode,
		CAST(EmergencyWard AS VARCHAR(MAX)) AS EmergencyWard,
		CAST(MaximumAge AS VARCHAR(MAX)) AS MaximumAge,
		CAST(MinimumAge AS VARCHAR(MAX)) AS MinimumAge,
		CAST(MultiplyBy AS VARCHAR(MAX)) AS MultiplyBy,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(SubtractFrom AS VARCHAR(MAX)) AS SubtractFrom,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidFromDate, 126) AS ValidFromDate,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_BillingFeeReductions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    