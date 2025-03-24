
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Resultat från analyser. De analyser som har samma maskintid (se kolumn MachineTime) tillhör samma version av svaret.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisComment': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Group': 'varchar(max)', 'GroupComment': 'varchar(max)', 'IsDeviating': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'MachineTime': 'varchar(max)', 'OrderComment': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferenceArea1': 'varchar(max)', 'ReferenceArea2': 'varchar(max)', 'ReferenceArea3': 'varchar(max)', 'ReferenceArea4': 'varchar(max)', 'ReplacementValue': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Unit': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': None, 'description': 'Labbets kod för analysen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tiden då laboratoriet skickade svaret.'}", 'MachineTime': "{'title_ui': 'Framställd', 'description': 'Tiden då labbsystemet skapade svaret.'}", 'Analysis': "{'title_ui': 'Analysnamn', 'description': 'Namn/beteckning för analys som utförts'}", 'ReplacementValue': '{\'title_ui\': \'Ersättningsvärde\', \'description\': "Används när svaret på analysen inte matchar analysvärdesfältet. Kan vara t ex, ett intervall, \'Saknas\', \'Utförd\'"}', 'Value': "{'title_ui': 'Resultat', 'description': 'Värde på resultatet av labanalysen. Ibland en sträng, ibland ett tal.'}", 'Unit': "{'title_ui': 'Enhet', 'description': 'Enhet för resultatet'}", 'ReferenceArea1': "{'title_ui': 'Referensintervall', 'description': 'Referensintervall 1'}", 'ReferenceArea2': "{'title_ui': 'Referensintervall2', 'description': 'Referensintervall 2 om det finns fler än 1'}", 'ReferenceArea3': "{'title_ui': 'Referensintervall3', 'description': 'Referensintervall 3 om det finns fler än 2'}", 'ReferenceArea4': "{'title_ui': 'Referensintervall4', 'description': 'Referensintervall 4 om det finns fler än 3'}", 'IsDeviating': "{'title_ui': '*', 'description': 'Om värdet är utanför referensintervall'}", 'AnalysisComment': "{'title_ui': None, 'description': 'Kommentar till labanalysen'}", 'Group': "{'title_ui': None, 'description': 'De analyser som har samma nummer grupperas tillsammans i svaret.'}", 'GroupComment': "{'title_ui': 'Gruppkommentar', 'description': 'Kommentar till den grupp som denna analys ligger i'}", 'OrderComment': "{'title_ui': 'Remisskommentar', 'description': 'Kommentar till remissen'}", 'IsEmergency': "{'title_ui': None, 'description': '0=Rutin, 1=Akut'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Group] AS VARCHAR(MAX)) AS [Group],
		CAST([GroupComment] AS VARCHAR(MAX)) AS [GroupComment],
		CAST([IsDeviating] AS VARCHAR(MAX)) AS [IsDeviating],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CONVERT(varchar(max), [MachineTime], 126) AS [MachineTime],
		CAST([OrderComment] AS VARCHAR(MAX)) AS [OrderComment],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ReferenceArea1] AS VARCHAR(MAX)) AS [ReferenceArea1],
		CAST([ReferenceArea2] AS VARCHAR(MAX)) AS [ReferenceArea2],
		CAST([ReferenceArea3] AS VARCHAR(MAX)) AS [ReferenceArea3],
		CAST([ReferenceArea4] AS VARCHAR(MAX)) AS [ReferenceArea4],
		CAST([ReplacementValue] AS VARCHAR(MAX)) AS [ReplacementValue],
		CONVERT(varchar(max), [ReplyTimestamp], 126) AS [ReplyTimestamp],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit],
		CAST([Value] AS VARCHAR(MAX)) AS [Value],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vChemLabReplies_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    