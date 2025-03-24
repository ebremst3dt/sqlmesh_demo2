
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Tabellen innehåller information om senaste inläsning till db för varje typ av data som går att definiera i Reader. De fem första kolumnerna specificerar data från Reader och Readers ini-fil: vilket data som Reader var inställd att försöka läsa, samt export-id för körningen. Dessa samt några statistikkolumner läses in in db av både gamla och nya Writer. Övriga kolumner (utom TimestampRead) sätts initialt till NULL. Parallell writer skriver till tabellen en andra gång, och då till kolumner som rör Writerns inläsning till db, efter att inläsning av en uppsättning exportfiler är klar.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DataPartitionCode': 'varchar(max)', 'DefaultDatabase': 'varchar(max)', 'ExportHasData': 'varchar(max)', 'ExportHeaderCompsInRange': 'varchar(max)', 'ExportHeaderRowsInRange': 'varchar(max)', 'ExportID': 'varchar(max)', 'ExportRecordsInRange': 'varchar(max)', 'ExportRecordsOpened': 'varchar(max)', 'ExportRecordsWithDoctype': 'varchar(max)', 'FailDatabase': 'varchar(max)', 'IsBatch': 'varchar(max)', 'IsLoaded': 'varchar(max)', 'LoadingRuntimeID': 'varchar(max)', 'LoadingRuntimeLockFile': 'varchar(max)', 'StagingDatabase': 'varchar(max)', 'TableGroupName': 'varchar(max)', 'TableGroupOriginCode': 'varchar(max)', 'TimestampLoaded': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'DataPartitionCode': "{'title_ui': None, 'description': 'Indikerar om det är journalpartition 1-6, misc-tabell, kodtabellerna eller tabell MetaData'}", 'TableGroupOriginCode': "{'title_ui': None, 'description': 'Id för dokumenttyp, men tabellgruppsnamn för misc-tabeller, kodtabeller och tabell MetaData'}", 'TableGroupName': "{'title_ui': None, 'description': 'Namn på tabellgruppen. Vissa tabellgrupper med journaldata motsvaras av flera dokumenttyper.'}", 'IsBatch': "{'title_ui': None, 'description': '1=Batch-körning 0=manuell körning'}", 'ExportID': "{'title_ui': None, 'description': 'Export-id för denna läsning från journalsystemet'}", 'LoadingRuntimeID': "{'title_ui': None, 'description': 'Id som särskiljer parallella writer-processer som laddar databasen. Sätts av parallell writer.'}", 'LoadingRuntimeLockFile': "{'title_ui': None, 'description': 'Sökväg till låsfil. Används av inläsande process internt. Sätts av parallell writer.'}", 'DefaultDatabase': "{'title_ui': None, 'description': 'Namn på måldatabasen. Sätts av parallell writer.'}", 'StagingDatabase': "{'title_ui': None, 'description': 'Namn på staging area-databasen som används av inläsande process. Sätts av parallell writer.'}", 'FailDatabase': "{'title_ui': None, 'description': 'Namn på fail-databasen som används av inläsande process. Sätts av parallell writer.'}", 'IsLoaded': "{'title_ui': None, 'description': 'Om inläsningen till databasen lyckats eller ej för delmängden data som raden specificerar. Sätts av parallell writer.'}", 'ExportHasData': "{'title_ui': None, 'description': 'Om exportfiler med data från Reader finns för delmängden data som raden specificerar. Sätts av parallell writer.'}", 'ExportRecordsOpened': "{'title_ui': None, 'description': 'Antal journalöppningar registrerade i exporten'}", 'ExportRecordsInRange': "{'title_ui': None, 'description': 'Statistik avsedd för prestandaoptimering internt CGM'}", 'ExportRecordsWithDoctype': "{'title_ui': None, 'description': 'Statistik avsedd för prestandaoptimering internt CGM'}", 'ExportHeaderCompsInRange': "{'title_ui': None, 'description': 'Statistik avsedd för prestandaoptimering internt CGM'}", 'ExportHeaderRowsInRange': "{'title_ui': None, 'description': 'Statistik avsedd för prestandaoptimering internt CGM'}", 'TimestampLoaded': "{'title_ui': None, 'description': 'Tidpunkt då inläsning av tabellgrupper med ExportID är avslutad. Sätts av parallell writer.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DataPartitionCode] AS VARCHAR(MAX)) AS [DataPartitionCode],
		CAST([DefaultDatabase] AS VARCHAR(MAX)) AS [DefaultDatabase],
		CAST([ExportHasData] AS VARCHAR(MAX)) AS [ExportHasData],
		CAST([ExportHeaderCompsInRange] AS VARCHAR(MAX)) AS [ExportHeaderCompsInRange],
		CAST([ExportHeaderRowsInRange] AS VARCHAR(MAX)) AS [ExportHeaderRowsInRange],
		CAST([ExportID] AS VARCHAR(MAX)) AS [ExportID],
		CAST([ExportRecordsInRange] AS VARCHAR(MAX)) AS [ExportRecordsInRange],
		CAST([ExportRecordsOpened] AS VARCHAR(MAX)) AS [ExportRecordsOpened],
		CAST([ExportRecordsWithDoctype] AS VARCHAR(MAX)) AS [ExportRecordsWithDoctype],
		CAST([FailDatabase] AS VARCHAR(MAX)) AS [FailDatabase],
		CAST([IsBatch] AS VARCHAR(MAX)) AS [IsBatch],
		CAST([IsLoaded] AS VARCHAR(MAX)) AS [IsLoaded],
		CAST([LoadingRuntimeID] AS VARCHAR(MAX)) AS [LoadingRuntimeID],
		CAST([LoadingRuntimeLockFile] AS VARCHAR(MAX)) AS [LoadingRuntimeLockFile],
		CAST([StagingDatabase] AS VARCHAR(MAX)) AS [StagingDatabase],
		CAST([TableGroupName] AS VARCHAR(MAX)) AS [TableGroupName],
		CAST([TableGroupOriginCode] AS VARCHAR(MAX)) AS [TableGroupOriginCode],
		CONVERT(varchar(max), [TimestampLoaded], 126) AS [TimestampLoaded],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vMetaDataLoad) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    