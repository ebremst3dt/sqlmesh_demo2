
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Resultat från analyser för virologi",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisExtID': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisShortName': 'varchar(max)', 'DocumentID': 'varchar(max)', 'LID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ResultRow': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'Specimen': 'varchar(max)', 'TestResult': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'ResultRow': "{'title_ui': None, 'description': 'Refererar till en post i VirologyReplies_Result'}", 'AnalysisID': "{'title_ui': 'Analys', 'description': 'Kod för vald analys (Labbets interna kod)'}", 'AnalysisExtID': "{'title_ui': 'Analys', 'description': 'Kod för vald analys (Labbets externa kod)'}", 'AnalysisShortName': "{'title_ui': 'Analys', 'description': 'Analysens kortnamn'}", 'Analysis': "{'title_ui': 'Analys', 'description': 'Vald analys i klartext'}", 'LID': "{'title_ui': 'prov-id', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system'}", 'Specimen': "{'title_ui': 'mtrl', 'description': 'Provmaterial'}", 'SamplingDate': "{'title_ui': 'provtagningsdatum', 'description': 'Provtagningsdatum'}", 'TestResult': "{'title_ui': 'Resultat', 'description': 'ex. Negativ, Positiv eller ett numeriskt värde'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Analysis] AS VARCHAR(MAX)) AS [Analysis],
		CAST([AnalysisExtID] AS VARCHAR(MAX)) AS [AnalysisExtID],
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([AnalysisShortName] AS VARCHAR(MAX)) AS [AnalysisShortName],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ResultRow] AS VARCHAR(MAX)) AS [ResultRow],
		CAST([SamplingDate] AS VARCHAR(MAX)) AS [SamplingDate],
		CAST([Specimen] AS VARCHAR(MAX)) AS [Specimen],
		CAST([TestResult] AS VARCHAR(MAX)) AS [TestResult],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vVirologyReplies_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    