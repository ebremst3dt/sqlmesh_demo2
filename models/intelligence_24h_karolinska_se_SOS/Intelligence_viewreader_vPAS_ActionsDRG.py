
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Angivna operations- och åtgärdskoder vid DRG (KVÅ-koder)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ActionCode': 'varchar(max)', 'ActionDate': 'varchar(max)', 'ActionID': 'varchar(max)', 'DiagnosisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ZATC1ActionCode': 'varchar(max)', 'ZATC2ActionCode': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'ActionID': "{'title_ui': None, 'description': 'Löpnummer'}", 'ActionCode': "{'title_ui': 'Op-/åtgärdskod', 'description': 'Operations- eller åtgärdskod'}", 'ActionDate': "{'title_ui': 'Op-/åtgärdsdatum', 'description': 'Datum för åtgärd (operationsdatum)'}", 'ZATC1ActionCode': "{'title_ui': 'Z/ATC-kod 1', 'description': 'Z- eller ATC-kod som förtydligar åtgärdskoden. Z-koder är operationskoder som börjar med Z.'}", 'ZATC2ActionCode': "{'title_ui': 'Z/ATC-kod 2', 'description': 'Z- eller ATC-kod som förtydligar åtgärdskoden. Z-koder är operationskoder som börjar med Z.'}", 'DiagnosisID': "{'title_ui': 'Op-/åtgärdsdiagnos', 'description': 'Vilken av diagnoserna som ligger till grund för åtgärden'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ActionCode] AS VARCHAR(MAX)) AS [ActionCode],
		CONVERT(varchar(max), [ActionDate], 126) AS [ActionDate],
		CAST([ActionID] AS VARCHAR(MAX)) AS [ActionID],
		CAST([DiagnosisID] AS VARCHAR(MAX)) AS [DiagnosisID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([ZATC1ActionCode] AS VARCHAR(MAX)) AS [ZATC1ActionCode],
		CAST([ZATC2ActionCode] AS VARCHAR(MAX)) AS [ZATC2ActionCode] 
	FROM Intelligence.viewreader.vPAS_ActionsDRG) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    