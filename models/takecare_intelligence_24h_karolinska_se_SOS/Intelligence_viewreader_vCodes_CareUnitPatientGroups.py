
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Vårdenhetens lista i sök/välj patient över befintliga patientgrupper. Ögonblicksbild då data extraheras från journalsystemet.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CreatedAtCareUnitID': 'varchar(max)', 'Group': 'varchar(max)', 'GroupRow': 'varchar(max)', 'IsAutoRemoved': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'CreatedAtCareUnitID': "{'title_ui': None, 'description': 'Vårdenhet gruppen finns på'}", 'GroupRow': "{'title_ui': None, 'description': {'break': None}}", 'Group': "{'title_ui': 'Grupp', 'description': None}", 'IsAutoRemoved': "{'title_ui': 'Ta bort grupp automatiskt när den blir tom', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(Group AS VARCHAR(MAX)) AS Group,
		CAST(GroupRow AS VARCHAR(MAX)) AS GroupRow,
		CAST(IsAutoRemoved AS VARCHAR(MAX)) AS IsAutoRemoved,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_CareUnitPatientGroups) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    