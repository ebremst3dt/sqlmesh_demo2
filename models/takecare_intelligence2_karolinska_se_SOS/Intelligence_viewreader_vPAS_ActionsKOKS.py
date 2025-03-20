
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Angivna åtgärdskoder vid KÖKS (KVÅ-koder)",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ActionCode': 'varchar(max)', 'ActionDate': 'varchar(max)', 'ActionID': 'varchar(max)', 'DiagnosisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'ActionID': "{'title_ui': None, 'description': 'Löpnummer'}", 'ActionCode': "{'title_ui': 'Åtgärdskod', 'description': 'Operations- eller åtgärdskod (ICD10)'}", 'ActionDate': "{'title_ui': None, 'description': 'Datum för åtgärd (alltid besöksdatum)'}", 'DiagnosisID': "{'title_ui': None, 'description': 'Vilken av diagnoserna som ligger till grund för åtgärden (alltid huvuddiagnosen för KÖKS)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(ActionCode AS VARCHAR(MAX)) AS ActionCode,
		CONVERT(varchar(max), ActionDate, 126) AS ActionDate,
		CAST(ActionID AS VARCHAR(MAX)) AS ActionID,
		CAST(DiagnosisID AS VARCHAR(MAX)) AS DiagnosisID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vPAS_ActionsKOKS) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    