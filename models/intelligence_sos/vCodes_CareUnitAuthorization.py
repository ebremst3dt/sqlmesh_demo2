
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Visar vilken vårdenhet som får läsa en annan vårdenhets dokument.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'IsAuthorized': 'varchar(max)', 'ReaderCareUnitID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'ReaderCareUnitID': "{'title_ui': None, 'description': 'Användare på denna vårdenhet får se dokument skapade på...'}", 'CreatedAtCareUnitID': "{'title_ui': None, 'description': 'denna vårdenhet om...'}", 'IsAuthorized': "{'title_ui': None, 'description': 'den här flaggan är satt till 1.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
    start=start,
    enabled=False
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
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([IsAuthorized] AS VARCHAR(MAX)) AS [IsAuthorized],
		CAST([ReaderCareUnitID] AS VARCHAR(MAX)) AS [ReaderCareUnitID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_CareUnitAuthorization) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    