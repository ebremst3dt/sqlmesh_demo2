
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Innehållet i listan Till Section, i Beställning röntgen. (KODAK)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'HasDropIn': 'varchar(max)', 'LabOrderSettingsID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ToSectionCode': 'varchar(max)', 'ToSectionID': 'varchar(max)', 'ToSectionName': 'varchar(max)', 'ToSectionType': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'LabOrderSettingsID': "{'title_ui': 'Id', 'description': 'Identifierar ett labb och dess inställningar'}", 'ToSectionID': "{'title_ui': 'Id', 'description': None}", 'ToSectionCode': "{'title_ui': 'Intern kod rtg', 'description': 'Intern kod på röntgen'}", 'ToSectionName': "{'title_ui': 'Till sektion', 'description': None}", 'ToSectionType': "{'title_ui': 'RTG/Isotop', 'description': {'break': [None, None]}}", 'HasDropIn': "{'title_ui': 'Drop In', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
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
		CAST([HasDropIn] AS VARCHAR(MAX)) AS [HasDropIn],
		CAST([LabOrderSettingsID] AS VARCHAR(MAX)) AS [LabOrderSettingsID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([ToSectionCode] AS VARCHAR(MAX)) AS [ToSectionCode],
		CAST([ToSectionID] AS VARCHAR(MAX)) AS [ToSectionID],
		CAST([ToSectionName] AS VARCHAR(MAX)) AS [ToSectionName],
		CAST([ToSectionType] AS VARCHAR(MAX)) AS [ToSectionType],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_KodakLabSections) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    