
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Platser eller rum som används av akutenheter",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'HospitalID': 'varchar(max)', 'IsActive': 'varchar(max)', 'LocationID': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'LocationID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': 'Namn', 'description': None}", 'IsActive': "{'title_ui': 'Giltig t.o.m.', 'description': 'Om platsen för tillfället kan användas och väljas i TakeCare'}", 'HospitalID': "{'title_ui': 'Inrättning', 'description': 'Rummet visas om användaren är inloggad på en vårdenhet med denna inrättningskod.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['LocationID']
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
		CAST([HospitalID] AS VARCHAR(MAX)) AS [HospitalID],
		CAST([IsActive] AS VARCHAR(MAX)) AS [IsActive],
		CAST([LocationID] AS VARCHAR(MAX)) AS [LocationID],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_EmergencyLocations) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    