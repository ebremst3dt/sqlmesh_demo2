
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Angivna diagnoser vid KÖKS. En av dem är huvuddiagnos, resten bidiagnoser.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DiagnosisCode': 'varchar(max)', 'DiagnosisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsMainDiagnosis': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'DiagnosisID': "{'title_ui': None, 'description': 'Löpnummer'}", 'DiagnosisCode': "{'title_ui': 'Huvuddiagnos+Bidiagnos X', 'description': 'Diagnoskod (ICD10 eller ICD10P)'}", 'IsMainDiagnosis': "{'title_ui': None, 'description': 'Om detta är huvuddiagnosen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DiagnosisCode AS VARCHAR(MAX)) AS DiagnosisCode,
		CAST(DiagnosisID AS VARCHAR(MAX)) AS DiagnosisID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(IsMainDiagnosis AS VARCHAR(MAX)) AS IsMainDiagnosis,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vPAS_DiagnosesKOKS) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    