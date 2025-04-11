
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Innehållet i listan Patienten kallas från, i Beställning röntgen. (SECTRA)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'LabOrderSettingsID': 'varchar(max)', 'PatientCalledFromID': 'varchar(max)', 'PatientCalledFromName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'LabOrderSettingsID': "{'title_ui': 'Id', 'description': 'Identifierar ett labb och dess inställningar'}", 'PatientCalledFromID': "{'title_ui': 'Id', 'description': None}", 'PatientCalledFromName': "{'title_ui': 'Namn', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['LabOrderSettingsID', 'PatientCalledFromID']
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
		CAST([LabOrderSettingsID] AS VARCHAR(MAX)) AS [LabOrderSettingsID],
		CAST([PatientCalledFromID] AS VARCHAR(MAX)) AS [PatientCalledFromID],
		CAST([PatientCalledFromName] AS VARCHAR(MAX)) AS [PatientCalledFromName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_SectraCalledFrom) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    