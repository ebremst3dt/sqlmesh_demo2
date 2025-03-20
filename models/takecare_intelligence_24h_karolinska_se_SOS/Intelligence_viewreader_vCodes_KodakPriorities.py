
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Innehållet i listan Prioritet, i Beställning röntgen och fysiologi. (KODAK)",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'LabOrderSettingsID': 'varchar(max)', 'PriorityCode': 'varchar(max)', 'PriorityID': 'varchar(max)', 'PriorityName': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'LabOrderSettingsID': "{'title_ui': 'Id', 'description': 'Identifierar ett labb och dess inställningar'}", 'PriorityID': "{'title_ui': 'Id', 'description': None}", 'PriorityCode': "{'title_ui': 'Kod', 'description': None}", 'PriorityName': "{'title_ui': 'Namn', 'description': 'Intern kod på röntgen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(LabOrderSettingsID AS VARCHAR(MAX)) AS LabOrderSettingsID,
		CAST(PriorityCode AS VARCHAR(MAX)) AS PriorityCode,
		CAST(PriorityID AS VARCHAR(MAX)) AS PriorityID,
		CAST(PriorityName AS VARCHAR(MAX)) AS PriorityName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_KodakPriorities) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    