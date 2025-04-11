
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Valda analyser. Skapades av det gränssnitt som slutade användas på Huddinge Sjukhus 2007.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'Batch': 'varchar(max)', 'BatchID': 'varchar(max)', 'ClinicalPicture': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Localization': 'varchar(max)', 'PatientID': 'varchar(max)', 'PriorityOrder': 'varchar(max)', 'Row': 'varchar(max)', 'Specimen': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'SpecimenID': "{'title_ui': 'Provmaterial', 'description': 'Kod för valt provmaterial'}", 'Specimen': "{'title_ui': 'Provmaterial', 'description': None}", 'AnalysisID': "{'title_ui': 'Analys', 'description': 'Kod för vald analys'}", 'Analysis': "{'title_ui': 'Analys', 'description': 'Vald analys i klartext'}", 'Localization': "{'title_ui': 'Lokalisation', 'description': 'Var på kroppen som provet tagits.'}", 'BatchID': "{'title_ui': None, 'description': None}", 'Batch': "{'title_ui': None, 'description': 'Om beställningens utgångspunkt är sjukdomsbild lagras inte paket'}", 'ClinicalPicture': "{'title_ui': 'Sjukdomsbild', 'description': 'Om beställningens utgångspunkt är sjukdomsbild lagras valet här istället för i Batch'}", 'PriorityOrder': "{'title_ui': 'Prio-ordn', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['AnalysisID', 'DocumentID', 'PatientID', 'Row', 'SpecimenID', 'Version']
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
		CAST([Analysis] AS VARCHAR(MAX)) AS [Analysis],
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([Batch] AS VARCHAR(MAX)) AS [Batch],
		CAST([BatchID] AS VARCHAR(MAX)) AS [BatchID],
		CAST([ClinicalPicture] AS VARCHAR(MAX)) AS [ClinicalPicture],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Localization] AS VARCHAR(MAX)) AS [Localization],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PriorityOrder] AS VARCHAR(MAX)) AS [PriorityOrder],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([Specimen] AS VARCHAR(MAX)) AS [Specimen],
		CAST([SpecimenID] AS VARCHAR(MAX)) AS [SpecimenID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vVirologyOrders_Analyses1) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    