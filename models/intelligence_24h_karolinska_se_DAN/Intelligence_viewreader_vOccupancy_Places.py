
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Dit patienter har utplacerats",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'PatientsPlaced': 'varchar(max)', 'PlacedAtCareUnitKombika': 'varchar(max)', 'ReportDate': 'varchar(max)', 'ReportTime': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'ReportDate': "{'title_ui': 'Datum', 'description': 'Datum för beläggningsdata'}", 'ReportTime': "{'title_ui': None, 'description': 'Klockslag då beläggningsdata-batch körts'}", 'CareUnitID': "{'title_ui': None, 'description': 'Vårdenhet som beläggningsdata gäller'}", 'PlacedAtCareUnitKombika': "{'title_ui': None, 'description': 'Där det finns utplacerade patienter (kombikakod/EXID)'}", 'PatientsPlaced': "{'title_ui': None, 'description': 'Antal utplacerade patienter för denna kombika'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([PatientsPlaced] AS VARCHAR(MAX)) AS [PatientsPlaced],
		CAST([PlacedAtCareUnitKombika] AS VARCHAR(MAX)) AS [PlacedAtCareUnitKombika],
		CONVERT(varchar(max), [ReportDate], 126) AS [ReportDate],
		CONVERT(varchar(max), [ReportTime], 126) AS [ReportTime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vOccupancy_Places) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    