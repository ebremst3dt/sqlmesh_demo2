
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Omvårdnadsbehov",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareNeedID': 'varchar(max)', 'IsActive': 'varchar(max)', 'Name': 'varchar(max)', 'ShortName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'CareNeedID': "{'title_ui': 'Id', 'description': 'ID för omvårdnadsbehov.'}", 'Name': "{'title_ui': 'Namn', 'description': 'Omvårdnadsbehov'}", 'ShortName': "{'title_ui': 'Kort namn', 'description': 'Det kortnamn som används för att presentera omv. behovet i akutliggaren.'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då omv. behovet är giltig'}", 'IsActive': "{'title_ui': None, 'description': 'Om detta omvårdnadsbehov används för tillfället.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['CareNeedID']
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
		CAST([CareNeedID] AS VARCHAR(MAX)) AS [CareNeedID],
		CAST([IsActive] AS VARCHAR(MAX)) AS [IsActive],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([ShortName] AS VARCHAR(MAX)) AS [ShortName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_EmergencyCareNeeds) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    