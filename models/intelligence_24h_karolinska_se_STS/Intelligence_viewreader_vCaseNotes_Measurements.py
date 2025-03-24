
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Riktiga mätvärden lagras i journaltext från våren 2006. Tidigare lagrades fritextvärden. Mätvärden finns även i tabellerna Measurements och Emergency_TriageVitalMeasurements.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'KeywordTermID': 'varchar(max)', 'PatientID': 'varchar(max)', 'TextValue': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Unit': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'KeywordTermID': "{'title_ui': None, 'description': 'Term-id för detta sökord'}", 'Value': "{'title_ui': 'Mätvärde', 'description': 'Numeriskt värde på nya mätvärden (fr.o.m. sommaren 2006)'}", 'TextValue': '{\'title_ui\': None, \'description\': \'Fritextmätvärde om termen inte har datatypen "mätvärde"\'}', 'Unit': "{'title_ui': None, 'description': 'Enhet för värdet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([KeywordTermID] AS VARCHAR(MAX)) AS [KeywordTermID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([TextValue] AS VARCHAR(MAX)) AS [TextValue],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit],
		CAST([Value] AS VARCHAR(MAX)) AS [Value],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vCaseNotes_Measurements) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    