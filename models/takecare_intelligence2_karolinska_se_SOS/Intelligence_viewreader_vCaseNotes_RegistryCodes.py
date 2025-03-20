
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Under vissa sökord kan framförallt diagnoser anges. Användaren kan söka i register för diagnoser, orsaker, åtgärder, ATC- och KÖKS-koder.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Code': 'varchar(max)', 'CodeTable': 'varchar(max)', 'DiagnosisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'KeywordTermID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Text': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'KeywordTermID': "{'title_ui': None, 'description': 'Term-id för detta sökord'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Code': "{'title_ui': None, 'description': 'Fritextkod som oftast kommer från något register'}", 'Text': "{'title_ui': None, 'description': 'Fritext, ofta översättning av koden ovan'}", 'Type': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'DiagnosisID': "{'title_ui': None, 'description': 'UUID för diagnosen hos patienten. Ny 2007-01, sätts endast för de koder som finns registrerade i diagnosmodulen.'}", 'CodeTable': "{'title_ui': None, 'description': 'Namn på kodtabell som koden hämtats från. Ny 2007-01, sätts endast för de koder som finns registrerade i diagnosmodulen.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Code AS VARCHAR(MAX)) AS Code,
		CAST(CodeTable AS VARCHAR(MAX)) AS CodeTable,
		CAST(DiagnosisID AS VARCHAR(MAX)) AS DiagnosisID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(KeywordTermID AS VARCHAR(MAX)) AS KeywordTermID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(Text AS VARCHAR(MAX)) AS Text,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Type AS VARCHAR(MAX)) AS Type,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vCaseNotes_RegistryCodes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    