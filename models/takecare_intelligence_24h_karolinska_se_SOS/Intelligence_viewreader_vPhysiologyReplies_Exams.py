
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställda undersökningar",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'Exam': 'varchar(max)', 'ExamDateTime': 'varchar(max)', 'ExamID': 'varchar(max)', 'MethodCode': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Template': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ExamID': "{'title_ui': 'Undersökning', 'description': 'Undersökningskod. Visas ej i TakeCare om svaret kommer från Huddinge fyslab.'}", 'Exam': "{'title_ui': 'Undersökning', 'description': 'Visas ej i TakeCare om svaret kommer från Huddinge fyslab.'}", 'Template': "{'title_ui': None, 'description': 'Mall'}", 'ExamDateTime': "{'title_ui': None, 'description': 'Datum och tid då undersökningen genomfördes. Endast Sectra'}", 'MethodCode': "{'title_ui': None, 'description': 'Metodkod. Endast Sectra'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Exam AS VARCHAR(MAX)) AS Exam,
		CONVERT(varchar(max), ExamDateTime, 126) AS ExamDateTime,
		CAST(ExamID AS VARCHAR(MAX)) AS ExamID,
		CAST(MethodCode AS VARCHAR(MAX)) AS MethodCode,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(Template AS VARCHAR(MAX)) AS Template,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPhysiologyReplies_Exams) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    