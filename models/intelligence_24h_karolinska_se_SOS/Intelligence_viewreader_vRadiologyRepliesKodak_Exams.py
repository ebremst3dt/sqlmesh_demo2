
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Resultat från undersökningar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CCode1': 'varchar(max)', 'CCode2': 'varchar(max)', 'CatCode1': 'varchar(max)', 'CatCode2': 'varchar(max)', 'DiagnosisCode': 'varchar(max)', 'DiagnosisText': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Exam': 'varchar(max)', 'ExamID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Side': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ExamID': "{'title_ui': 'Undersökningskod', 'description': None}", 'Exam': "{'title_ui': None, 'description': 'Undersökning'}", 'Side': "{'title_ui': None, 'description': 'Sida'}", 'CatCode1': "{'title_ui': 'Rek. C-koder', 'description': None}", 'CCode1': "{'title_ui': 'Rek. C-koder', 'description': 'Viktklass 1'}", 'CatCode2': "{'title_ui': 'Rek. C-koder', 'description': None}", 'CCode2': "{'title_ui': 'Rek. C-koder', 'description': 'Viktklass 2'}", 'DiagnosisCode': "{'title_ui': None, 'description': 'Diagnos'}", 'DiagnosisText': "{'title_ui': None, 'description': 'Diagnos'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CCode1] AS VARCHAR(MAX)) AS [CCode1],
		CAST([CCode2] AS VARCHAR(MAX)) AS [CCode2],
		CAST([CatCode1] AS VARCHAR(MAX)) AS [CatCode1],
		CAST([CatCode2] AS VARCHAR(MAX)) AS [CatCode2],
		CAST([DiagnosisCode] AS VARCHAR(MAX)) AS [DiagnosisCode],
		CAST([DiagnosisText] AS VARCHAR(MAX)) AS [DiagnosisText],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Exam] AS VARCHAR(MAX)) AS [Exam],
		CAST([ExamID] AS VARCHAR(MAX)) AS [ExamID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([Side] AS VARCHAR(MAX)) AS [Side],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vRadiologyRepliesKodak_Exams) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    