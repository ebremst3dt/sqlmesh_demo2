
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Externa enheter. Tabellen kopplar ihop internt id för externa enheter (EXID) med enhetens övriga externa idn. EXID lagras i Kombika-kolumnen i alla andra tabeller. EXID är alltid 11 tecken lång, och har i Stockholm samma värde som Kombika. Enhet som tagits bort ur textfilen som laddat registret återfinns i tabellen, men endast EXID och huvud-id är skilda från NULL för den borttagna enheten.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CostCenter': 'varchar(max)', 'EAN': 'varchar(max)', 'EXID': 'varchar(max)', 'HSAID': 'varchar(max)', 'Kombika': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'EXID': "{'title_ui': None, 'description': 'Internt id'}", 'HSAID': "{'title_ui': None, 'description': None}", 'Kombika': "{'title_ui': None, 'description': None}", 'EAN': "{'title_ui': None, 'description': None}", 'CostCenter': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['EXID']
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
		CAST([CostCenter] AS VARCHAR(MAX)) AS [CostCenter],
		CAST([EAN] AS VARCHAR(MAX)) AS [EAN],
		CAST([EXID] AS VARCHAR(MAX)) AS [EXID],
		CAST([HSAID] AS VARCHAR(MAX)) AS [HSAID],
		CAST([Kombika] AS VARCHAR(MAX)) AS [Kombika],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_ExternalUnitIDS) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    