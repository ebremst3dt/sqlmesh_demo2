
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Undersökningar som kan kopplas till en beställning (Fysiologi)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Examination': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'HasEKGRegistration': 'varchar(max)', 'IsActive': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'RISCode': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'ExaminationID': "{'title_ui': None, 'description': 'Undersökningsid'}", 'Examination': "{'title_ui': None, 'description': 'Undersökningens namn'}", 'RISCode': "{'title_ui': None, 'description': 'RIS-kod'}", 'HasEKGRegistration': "{'title_ui': None, 'description': 'Ska markera i beställningen att EKG har registrerats'}", 'IsActive': "{'title_ui': None, 'description': 'Beställningsbar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['ExaminationID', 'OrderRegistryFileName']
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
		CAST([Examination] AS VARCHAR(MAX)) AS [Examination],
		CAST([ExaminationID] AS VARCHAR(MAX)) AS [ExaminationID],
		CAST([HasEKGRegistration] AS VARCHAR(MAX)) AS [HasEKGRegistration],
		CAST([IsActive] AS VARCHAR(MAX)) AS [IsActive],
		CAST([OrderRegistryFileName] AS VARCHAR(MAX)) AS [OrderRegistryFileName],
		CAST([RISCode] AS VARCHAR(MAX)) AS [RISCode],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_PhysiologyExams) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    