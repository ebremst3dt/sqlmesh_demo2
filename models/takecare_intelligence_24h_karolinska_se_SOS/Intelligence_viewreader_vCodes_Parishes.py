
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Län och Kommun församlingar.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CompositeID': 'varchar(max)', 'CountyID': 'varchar(max)', 'MunicipalityID': 'varchar(max)', 'ParishID': 'varchar(max)', 'ParishName': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'CountyID': "{'title_ui': None, 'description': 'Länskod'}", 'MunicipalityID': "{'title_ui': None, 'description': 'Kommunkod'}", 'ParishID': "{'title_ui': None, 'description': 'Församlingskod'}", 'CompositeID': "{'title_ui': None, 'description': 'Län+kommunkod+Församlingskod tillsammans'}", 'ParishName': "{'title_ui': None, 'description': 'Församling'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CompositeID AS VARCHAR(MAX)) AS CompositeID,
		CAST(CountyID AS VARCHAR(MAX)) AS CountyID,
		CAST(MunicipalityID AS VARCHAR(MAX)) AS MunicipalityID,
		CAST(ParishID AS VARCHAR(MAX)) AS ParishID,
		CAST(ParishName AS VARCHAR(MAX)) AS ParishName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_Parishes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    