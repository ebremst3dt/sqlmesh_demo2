
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read

    
@model(
    description="Analyser som kan kopplas till en beställning (farmlab), nedlagd, användes till april 2010",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATC1': 'varchar(max)', 'ATC2': 'varchar(max)', 'ATC3': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'IsLatestDoseRequired': 'varchar(max)', 'IsPatientCreatinineRequired': 'varchar(max)', 'IsPatientLengthRequired': 'varchar(max)', 'IsPatientSmokerRequired': 'varchar(max)', 'IsPatientWeightRequired': 'varchar(max)', 'TestCauseCode': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)'},
    column_descriptions={'AnalysisID': "{'title_ui': 'Substanser som ska analyseras', 'description': 'Kod för analys'}", 'Analysis': "{'title_ui': 'Substanser som ska analyseras', 'description': 'Analys/substans i klartext'}", 'IsPatientLengthRequired': "{'title_ui': None, 'description': 'Längd måste fyllas i'}", 'IsPatientWeightRequired': "{'title_ui': None, 'description': 'Vikt måste fyllas i'}", 'IsPatientCreatinineRequired': "{'title_ui': None, 'description': 'P-Kreatinin måste fyllas i'}", 'IsPatientSmokerRequired': "{'title_ui': None, 'description': 'Rökare måste anges'}", 'IsLatestDoseRequired': "{'title_ui': None, 'description': 'Senaste dos måste fyllas i'}", 'TubeID': "{'title_ui': 'Rörnamn', 'description': 'Rörkod'}", 'TestCauseCode': "{'title_ui': 'Orsak till provtagning', 'description': 'Orsak till provtagning, kod'}", 'ATC1': "{'title_ui': None, 'description': 'ATCkod'}", 'ATC2': "{'title_ui': None, 'description': 'ATCkod'}", 'ATC3': "{'title_ui': None, 'description': 'ATCkod'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ATC1] AS VARCHAR(MAX)) AS [ATC1],
		CAST([ATC2] AS VARCHAR(MAX)) AS [ATC2],
		CAST([ATC3] AS VARCHAR(MAX)) AS [ATC3],
		CAST([Analysis] AS VARCHAR(MAX)) AS [Analysis],
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([IsLatestDoseRequired] AS VARCHAR(MAX)) AS [IsLatestDoseRequired],
		CAST([IsPatientCreatinineRequired] AS VARCHAR(MAX)) AS [IsPatientCreatinineRequired],
		CAST([IsPatientLengthRequired] AS VARCHAR(MAX)) AS [IsPatientLengthRequired],
		CAST([IsPatientSmokerRequired] AS VARCHAR(MAX)) AS [IsPatientSmokerRequired],
		CAST([IsPatientWeightRequired] AS VARCHAR(MAX)) AS [IsPatientWeightRequired],
		CAST([TestCauseCode] AS VARCHAR(MAX)) AS [TestCauseCode],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TubeID] AS VARCHAR(MAX)) AS [TubeID] 
	FROM Intelligence.viewreader.vCodes_PharmAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    