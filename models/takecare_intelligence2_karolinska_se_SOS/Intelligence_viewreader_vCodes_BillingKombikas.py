
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Ekonomiska kombikor och vårdenhetsnamn. Kan kopplas till kassor i Codes_BillingCounters genom kopplingstabellen Codes_BillingCounterKombikas.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'EconomicalKombika': 'varchar(max)', 'EconomicalKombikaName': 'varchar(max)', 'EconomicalKombikaRow': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'EconomicalKombikaRow': "{'title_ui': None, 'description': 'Används som id för kombinationen av kombikakod och kombikanamn. Internt id som kan förändras.'}", 'EconomicalKombika': "{'title_ui': 'Kombika', 'description': 'Ekonomisk Kombika/EXID, kod'}", 'EconomicalKombikaName': "{'title_ui': 'Ekonomisk enhet', 'description': 'Ekonomisk Kombika, namn'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(EconomicalKombika AS VARCHAR(MAX)) AS EconomicalKombika,
		CAST(EconomicalKombikaName AS VARCHAR(MAX)) AS EconomicalKombikaName,
		CAST(EconomicalKombikaRow AS VARCHAR(MAX)) AS EconomicalKombikaRow,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_BillingKombikas) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    