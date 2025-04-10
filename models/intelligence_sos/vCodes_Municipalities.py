
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Län och Kommuner. En kombination av kodtabellen 'Län och Kommuner' samt tabellen 'Län och Kommuner - Specialkoder' i generella register. Endast unika län/kommunkoder lagras. Om dubletter förekommer används i första hand det som angetts i generella register och den som inte har ett t.o.m datum, eller den med det senaste t.o.m datumet.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompositeID': 'varchar(max)', 'CountyID': 'varchar(max)', 'CountyName': 'varchar(max)', 'MunicipalityID': 'varchar(max)', 'MunicipalityName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'CountyID': "{'title_ui': None, 'description': 'Länskod'}", 'MunicipalityID': "{'title_ui': None, 'description': 'Kommunkod'}", 'CompositeID': "{'title_ui': None, 'description': 'Län+kommunkod tillsammans'}", 'CountyName': "{'title_ui': None, 'description': 'Län'}", 'MunicipalityName': "{'title_ui': None, 'description': 'Kommun'}", 'ValidThroughDate': "{'title_ui': None, 'description': 'T.o.m datum'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CompositeID] AS VARCHAR(MAX)) AS [CompositeID],
		CAST([CountyID] AS VARCHAR(MAX)) AS [CountyID],
		CAST([CountyName] AS VARCHAR(MAX)) AS [CountyName],
		CAST([MunicipalityID] AS VARCHAR(MAX)) AS [MunicipalityID],
		CAST([MunicipalityName] AS VARCHAR(MAX)) AS [MunicipalityName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_Municipalities) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    