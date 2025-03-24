
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Behörighetsgivande vårdenhetskolumner som hör till tabellen",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ACLColumnName': 'varchar(max)', 'ACLTableName': 'varchar(max)', 'IsFromExport': 'varchar(max)', 'TableName': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'TableName': "{'title_ui': None, 'description': 'Tabellens namn'}", 'IsFromExport': "{'title_ui': None, 'description': 'Om metadata fanns i exporten eller om det är manuellt tillagt statiskt (ev. föråldrat) data'}", 'ACLColumnName': "{'title_ui': None, 'description': 'Namn på en behörighetsgivande kolumn i tabellen'}", 'ACLTableName': "{'title_ui': None, 'description': 'Tabell där de behörighetsgivande kolumnerna finns (kan vara annan än denna tabell)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(ACLColumnName AS VARCHAR(MAX)) AS ACLColumnName,
		CAST(ACLTableName AS VARCHAR(MAX)) AS ACLTableName,
		CAST(IsFromExport AS VARCHAR(MAX)) AS IsFromExport,
		CAST(TableName AS VARCHAR(MAX)) AS TableName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vMetaDataTables_ACLColumns) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    