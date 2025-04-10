
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Odlingsdata",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisRow': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Finding': 'varchar(max)', 'FindingCode': 'varchar(max)', 'Growth': 'varchar(max)', 'IsPathological': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Signature': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': None, 'description': 'Labbets kod för analysen'}", 'AnalysisRow': "{'title_ui': None, 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Finding': "{'title_ui': 'Resultat', 'description': 'Odlingsfynd'}", 'FindingCode': "{'title_ui': 'Resultat', 'description': 'Labbets interna kod för odlingsfyndet'}", 'Growth': "{'title_ui': 'Växt', 'description': 'Växt. Visas som en odlingskommentar'}", 'Comment': "{'title_ui': 'Resultat', 'description': 'Odlingskommentar'}", 'Signature': "{'title_ui': None, 'description': 'Signatur'}", 'IsPathological': "{'title_ui': None, 'description': 'Om fyndet är patologiskt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['AnalysisID', 'AnalysisRow', 'DocumentID', 'PatientID', 'Row', 'Version']
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
		CAST([AnalysisRow] AS VARCHAR(MAX)) AS [AnalysisRow],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Finding] AS VARCHAR(MAX)) AS [Finding],
		CAST([FindingCode] AS VARCHAR(MAX)) AS [FindingCode],
		CAST([Growth] AS VARCHAR(MAX)) AS [Growth],
		CAST([IsPathological] AS VARCHAR(MAX)) AS [IsPathological],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([Signature] AS VARCHAR(MAX)) AS [Signature],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMultiLabReplies_Cultures) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    