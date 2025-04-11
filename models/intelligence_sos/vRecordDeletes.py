
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Journaler som ska laddas om helt för att ta bort raderade dokument. Är alltid tom i databasen p.g.a. GDPR",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DeleteID': 'varchar(max)', 'ExportID': 'varchar(max)', 'PatientID': 'varchar(max)', 'RecordPartition': 'varchar(max)', 'TimestampDeleted': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'DeleteID': "{'title_ui': None, 'description': 'Unikt id för omläsningen'}", 'PatientID': "{'title_ui': None, 'description': 'Journal där data ska läsas om helt'}", 'TimestampDeleted': "{'title_ui': None, 'description': 'Tidpunkt då omläsning beställdes'}", 'ExportID': "{'title_ui': None, 'description': None}", 'RecordPartition': "{'title_ui': None, 'description': 'Journalpartition som journalen hör till'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DeleteID']
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
		CAST([DeleteID] AS VARCHAR(MAX)) AS [DeleteID],
		CAST([ExportID] AS VARCHAR(MAX)) AS [ExportID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([RecordPartition] AS VARCHAR(MAX)) AS [RecordPartition],
		CONVERT(varchar(max), [TimestampDeleted], 126) AS [TimestampDeleted],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vRecordDeletes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    