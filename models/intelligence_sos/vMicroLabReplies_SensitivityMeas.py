
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Resistensbestämning för mikrolabbsvar.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AntimicrobialAgent': 'varchar(max)', 'DocumentID': 'varchar(max)', 'MICMeasurement': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SIRCategory': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': 'SVAR', 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'AntimicrobialAgent': "{'title_ui': 'Resistensbestämning', 'description': 'Antimikrobiellt medel. Innehåller även ett ordningsnummer som används för sortering.'}", 'SIRCategory': "{'title_ui': None, 'description': 'SIR-Kategori'}", 'MICMeasurement': "{'title_ui': 'MIC-värden', 'description': 'MIC-värde'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['AnalysisID', 'DocumentID', 'PatientID', 'Row', 'Version']
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
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([AntimicrobialAgent] AS VARCHAR(MAX)) AS [AntimicrobialAgent],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([MICMeasurement] AS VARCHAR(MAX)) AS [MICMeasurement],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SIRCategory] AS VARCHAR(MAX)) AS [SIRCategory],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMicroLabReplies_SensitivityMeas) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    