
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Resistenser""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AnalysisID': 'varchar(max)', 'AnalysisRow': 'varchar(max)', 'Antibiotics': 'varchar(max)', 'AntibioticsCode': 'varchar(max)', 'Comment': 'varchar(max)', 'CultureRow': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Mesurement': 'varchar(max)', 'PatientID': 'varchar(max)', 'ResistenceType': 'varchar(max)', 'Row': 'varchar(max)', 'SIRCategory': 'varchar(max)', 'Signature': 'varchar(max)', 'SortOrder': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': None, 'description': 'Labbets kod för analysen'}", 'AnalysisRow': "{'title_ui': None, 'description': None}", 'CultureRow': "{'title_ui': None, 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Antibiotics': "{'title_ui': 'Resistenstabell', 'description': 'Antimikrobiellt medel'}", 'AntibioticsCode': "{'title_ui': 'Resistenstabell', 'description': 'Kod för antimikrobiellt medel'}", 'ResistenceType': "{'title_ui': None, 'description': {'break': [None, None]}}", 'Mesurement': "{'title_ui': '*Fynd', 'description': 'Mätvärde. Visas endast för MIC-värden'}", 'SIRCategory': "{'title_ui': None, 'description': 'SIR-Kategori'}", 'Comment': "{'title_ui': '*Fynd', 'description': 'Resistenskommentar'}", 'Signature': "{'title_ui': None, 'description': 'Signatur'}", 'SortOrder': "{'title_ui': None, 'description': 'Innehåller ett ordningsnummer som används för sortering'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AnalysisID AS VARCHAR(MAX)) AS AnalysisID,
		CAST(AnalysisRow AS VARCHAR(MAX)) AS AnalysisRow,
		CAST(Antibiotics AS VARCHAR(MAX)) AS Antibiotics,
		CAST(AntibioticsCode AS VARCHAR(MAX)) AS AntibioticsCode,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CultureRow AS VARCHAR(MAX)) AS CultureRow,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Mesurement AS VARCHAR(MAX)) AS Mesurement,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ResistenceType AS VARCHAR(MAX)) AS ResistenceType,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SIRCategory AS VARCHAR(MAX)) AS SIRCategory,
		CAST(Signature AS VARCHAR(MAX)) AS Signature,
		CAST(SortOrder AS VARCHAR(MAX)) AS SortOrder,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMultiLabReplies_Resistences) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    