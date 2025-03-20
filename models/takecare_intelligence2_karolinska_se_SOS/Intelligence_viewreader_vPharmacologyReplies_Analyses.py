
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Analyser.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Analysis': 'varchar(max)', 'AnalysisComment': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisRow': 'varchar(max)', 'DocumentID': 'varchar(max)', 'GroupRow': 'varchar(max)', 'IsDeviating': 'varchar(max)', 'PatientID': 'varchar(max)', 'Priority': 'varchar(max)', 'ReferenceArea1': 'varchar(max)', 'ReferenceArea2': 'varchar(max)', 'ReplacementValue': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Unit': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'GroupRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'AnalysisRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'AnalysisID': "{'title_ui': None, 'description': 'Kod för analysen'}", 'Analysis': "{'title_ui': None, 'description': 'Namn/beteckning för analys som utförts'}", 'Value': "{'title_ui': None, 'description': 'Värde på resultatet av senaste labanalysen. Ibland en sträng, ibland ett tal.'}", 'Unit': "{'title_ui': 'Enhet', 'description': 'Enhet för resultatet'}", 'ReplacementValue': "{'title_ui': None, 'description': 'Användes när svaret på analysen inte matchar analysvärdesfältet. Endast svar t.o.m. 2004'}", 'ReferenceArea1': "{'title_ui': 'Riktvärde', 'description': 'Referensintervall 1'}", 'ReferenceArea2': "{'title_ui': 'Riktvärde', 'description': 'Referensintervall 2 om det finns fler än 1'}", 'IsDeviating': "{'title_ui': '*', 'description': 'Om senaste värdet är utanför referensintervall'}", 'AnalysisComment': "{'title_ui': 'Laboratorie kommentar', 'description': 'Kommentar till labanalysen'}", 'Priority': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily"
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(Analysis AS VARCHAR(MAX)) AS Analysis,
		CAST(AnalysisComment AS VARCHAR(MAX)) AS AnalysisComment,
		CAST(AnalysisID AS VARCHAR(MAX)) AS AnalysisID,
		CAST(AnalysisRow AS VARCHAR(MAX)) AS AnalysisRow,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(GroupRow AS VARCHAR(MAX)) AS GroupRow,
		CAST(IsDeviating AS VARCHAR(MAX)) AS IsDeviating,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Priority AS VARCHAR(MAX)) AS Priority,
		CAST(ReferenceArea1 AS VARCHAR(MAX)) AS ReferenceArea1,
		CAST(ReferenceArea2 AS VARCHAR(MAX)) AS ReferenceArea2,
		CAST(ReplacementValue AS VARCHAR(MAX)) AS ReplacementValue,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Unit AS VARCHAR(MAX)) AS Unit,
		CAST(Value AS VARCHAR(MAX)) AS Value,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPharmacologyReplies_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    