
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""I denna tabell lagras kopplad KVÅ kod för hälsoproblem i de fall de existarar för dokumentet""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ActionID': 'varchar(max)', 'ActionName': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'LinkedDocumentCreatedAtCareUnitID': 'varchar(max)', 'LinkedDocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'EventDate': "{'title_ui': 'Datum', 'description': 'Händelsetid för åtgärd'}", 'EventTime': "{'title_ui': 'Datum', 'description': 'Händelsetid för åtgärd'}", 'ActionID': "{'title_ui': 'Åtgärdskod', 'description': 'Åtgärdskod (KVÅ-kod)'}", 'ActionName': "{'title_ui': 'Åtgärd', 'description': 'Åtgärdsbeskrivning'}", 'LinkedDocumentID': "{'title_ui': None, 'description': 'Dokument-id på journaltexten åtgärden hämtades från eller specialkod (-1=Okänd, -2=Annan vårdgivare, -3=Annan utländsk vårdgivare)'}", 'LinkedDocumentCreatedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Skapad på'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(ActionID AS VARCHAR(MAX)) AS ActionID,
		CAST(ActionName AS VARCHAR(MAX)) AS ActionName,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CONVERT(varchar(max), EventDate, 126) AS EventDate,
		CONVERT(varchar(max), EventTime, 126) AS EventTime,
		CAST(LinkedDocumentCreatedAtCareUnitID AS VARCHAR(MAX)) AS LinkedDocumentCreatedAtCareUnitID,
		CAST(LinkedDocumentID AS VARCHAR(MAX)) AS LinkedDocumentID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vHealthIssues_Actions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    