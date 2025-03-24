
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställningsspec. En specifikation består av en unik kombination av flera men inte alltid alla av följande: analys, undersökning, rör, provmaterial. (MultiLabb analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'IsAcute': 'varchar(max)', 'IsDrugInfoRequired': 'varchar(max)', 'IsLocalizationRequired': 'varchar(max)', 'IsRoutine': 'varchar(max)', 'NoOfLabels': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'OrderableID': 'varchar(max)', 'SectionCode': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'OrderableID': "{'title_ui': None, 'description': 'Beställningsspecifikationsid'}", 'SpecimenID': "{'title_ui': None, 'description': 'Provmaterialid'}", 'ExaminationID': "{'title_ui': None, 'description': 'Undersökningsid'}", 'AnalysisID': "{'title_ui': None, 'description': 'Analysid'}", 'TubeID': "{'title_ui': None, 'description': 'Rörid'}", 'IsLocalizationRequired': "{'title_ui': None, 'description': 'Lokalisation ska visas och måste fyllas i'}", 'NoOfLabels': "{'title_ui': None, 'description': 'Antal etiketter'}", 'SectionCode': "{'title_ui': None, 'description': 'Sektionskod'}", 'IsAcute': "{'title_ui': None, 'description': 'Om både IsAcute och IsRoutine är False så ingår analysen i en profil och kan inte beställas separat.'}", 'IsRoutine': "{'title_ui': None, 'description': 'Om både IsAcute och IsRoutine är False så ingår analysen i en profil och kan inte beställas separat.'}", 'IsDrugInfoRequired': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([ExaminationID] AS VARCHAR(MAX)) AS [ExaminationID],
		CAST([IsAcute] AS VARCHAR(MAX)) AS [IsAcute],
		CAST([IsDrugInfoRequired] AS VARCHAR(MAX)) AS [IsDrugInfoRequired],
		CAST([IsLocalizationRequired] AS VARCHAR(MAX)) AS [IsLocalizationRequired],
		CAST([IsRoutine] AS VARCHAR(MAX)) AS [IsRoutine],
		CAST([NoOfLabels] AS VARCHAR(MAX)) AS [NoOfLabels],
		CAST([OrderRegistryFileName] AS VARCHAR(MAX)) AS [OrderRegistryFileName],
		CAST([OrderableID] AS VARCHAR(MAX)) AS [OrderableID],
		CAST([SectionCode] AS VARCHAR(MAX)) AS [SectionCode],
		CAST([SpecimenID] AS VARCHAR(MAX)) AS [SpecimenID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TubeID] AS VARCHAR(MAX)) AS [TubeID] 
	FROM Intelligence.viewreader.vCodes_MultiLabOrderables) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    