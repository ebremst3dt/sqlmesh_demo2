
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisComment': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsDeviating': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferenceArea': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Unit': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': 'Analys', 'description': 'Kod för vald analys'}", 'Analysis': "{'title_ui': 'Analys', 'description': 'Vald analys i klartext'}", 'Value': "{'title_ui': 'Resultat', 'description': 'Värde på resultatet av labanalysen. Ibland en sträng, ibland ett tal.'}", 'Unit': "{'title_ui': 'Enhet', 'description': 'Enhet för resultatet. Pga. ett tidigare problem med hopblandning av enheter, i filerna från labb, tas endast de enheter med som kan verifieras. Felet rättades 20081008 och efter det tas alltid enhet med. För svar sparade före rättningen, tas enheten med för alla svar med endast en analys.'}", 'IsDeviating': "{'title_ui': '*', 'description': 'Om värdet är utanför referensintervall'}", 'ReferenceArea': "{'title_ui': 'Ref.intervall', 'description': None}", 'AnalysisComment': "{'title_ui': None, 'description': 'Kommentar till labanalysen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AnalysisComment] AS VARCHAR(MAX)) AS [AnalysisComment],
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([IsDeviating] AS VARCHAR(MAX)) AS [IsDeviating],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ReferenceArea] AS VARCHAR(MAX)) AS [ReferenceArea],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit],
		CAST([Value] AS VARCHAR(MAX)) AS [Value],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vImmLabRepliesProsang_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    