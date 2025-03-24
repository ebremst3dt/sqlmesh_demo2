
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Meta-data med lite statistik och information från körningarna av Intelligence",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DataFiles': 'varchar(max)', 'ExportID': 'varchar(max)', 'FromDatetime': 'varchar(max)', 'HasCodeTables': 'varchar(max)', 'IsBatch': 'varchar(max)', 'IsEncrypted': 'varchar(max)', 'IsReload': 'varchar(max)', 'MetaFiles': 'varchar(max)', 'MiscTables': 'varchar(max)', 'ReaderEndDatetime': 'varchar(max)', 'ReaderStartDatetime': 'varchar(max)', 'ReaderVersion': 'varchar(max)', 'RecordPartitions': 'varchar(max)', 'RecordsAttemptedToOpen': 'varchar(max)', 'RecordsOpened': 'varchar(max)', 'RunComment': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ToDatetime': 'varchar(max)', 'WriterVersion': 'varchar(max)'},
    column_descriptions={'ExportID': "{'title_ui': None, 'description': 'ID för körningen av Reader'}", 'ReaderVersion': "{'title_ui': None, 'description': 'Version av Reader som kördes'}", 'WriterVersion': "{'title_ui': None, 'description': 'Version av Writer som kördes'}", 'IsEncrypted': "{'title_ui': None, 'description': 'Om kryptering är påslaget'}", 'RecordsAttemptedToOpen': "{'title_ui': None, 'description': 'Antal journaler som skulle exporteras'}", 'RecordsOpened': "{'title_ui': None, 'description': 'Antal journaler som faktiskt öppnades av Readern'}", 'FromDatetime': "{'title_ui': None, 'description': 'Starttid för körningen (inklusive)'}", 'ToDatetime': "{'title_ui': None, 'description': 'Sluttid för körningen (inklusive)'}", 'ReaderStartDatetime': "{'title_ui': None, 'description': 'Tidpunkt då Reader-körningen startades'}", 'ReaderEndDatetime': "{'title_ui': None, 'description': 'Tidpunkt då Reader-körningen avslutades'}", 'DataFiles': "{'title_ui': None, 'description': 'Antal skrivna BCP-filer med data'}", 'MetaFiles': "{'title_ui': None, 'description': 'Antal skrivna XML-filer med metadata (tillika antal tabeller, inklusive denna)'}", 'HasCodeTables': "{'title_ui': None, 'description': 'Om kodtabellerna exporterades'}", 'RecordPartitions': "{'title_ui': None, 'description': 'De journalpartitioner som har exporterats av körningen (kommaseparerade)'}", 'MiscTables': "{'title_ui': None, 'description': 'De diversetabeller som har valts vid körningen (kommaseparerade)'}", 'IsBatch': "{'title_ui': None, 'description': '1=Batch-körning 0=manuell körning'}", 'RunComment': "{'title_ui': None, 'description': 'Fritextkommentar satt vid start av körning'}", 'IsReload': "{'title_ui': None, 'description': '1=Omladdning 0=ej omladdning'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DataFiles] AS VARCHAR(MAX)) AS [DataFiles],
		CAST([ExportID] AS VARCHAR(MAX)) AS [ExportID],
		CONVERT(varchar(max), [FromDatetime], 126) AS [FromDatetime],
		CAST([HasCodeTables] AS VARCHAR(MAX)) AS [HasCodeTables],
		CAST([IsBatch] AS VARCHAR(MAX)) AS [IsBatch],
		CAST([IsEncrypted] AS VARCHAR(MAX)) AS [IsEncrypted],
		CAST([IsReload] AS VARCHAR(MAX)) AS [IsReload],
		CAST([MetaFiles] AS VARCHAR(MAX)) AS [MetaFiles],
		CAST([MiscTables] AS VARCHAR(MAX)) AS [MiscTables],
		CONVERT(varchar(max), [ReaderEndDatetime], 126) AS [ReaderEndDatetime],
		CONVERT(varchar(max), [ReaderStartDatetime], 126) AS [ReaderStartDatetime],
		CAST([ReaderVersion] AS VARCHAR(MAX)) AS [ReaderVersion],
		CAST([RecordPartitions] AS VARCHAR(MAX)) AS [RecordPartitions],
		CAST([RecordsAttemptedToOpen] AS VARCHAR(MAX)) AS [RecordsAttemptedToOpen],
		CAST([RecordsOpened] AS VARCHAR(MAX)) AS [RecordsOpened],
		CAST([RunComment] AS VARCHAR(MAX)) AS [RunComment],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ToDatetime], 126) AS [ToDatetime],
		CAST([WriterVersion] AS VARCHAR(MAX)) AS [WriterVersion] 
	FROM Intelligence.viewreader.vMetaData) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    