
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Angivna diagnoser vid DRG. En av dem är huvuddiagnos, resten bidiagnoser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Cause1': 'varchar(max)', 'Cause2': 'varchar(max)', 'DiagnosisCode': 'varchar(max)', 'DiagnosisID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EtiologicDiagnosisID': 'varchar(max)', 'IsCauseOfDeath': 'varchar(max)', 'IsMainDiagnosis': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'DiagnosisID': "{'title_ui': None, 'description': 'Löpnummer'}", 'DiagnosisCode': "{'title_ui': 'Diagnos/ATC-kod', 'description': 'Diagnoskod (ICD10, ICD10P eller ATC)'}", 'IsMainDiagnosis': "{'title_ui': 'Huvuddiagnos', 'description': 'Om detta är huvuddiagnosen'}", 'EtiologicDiagnosisID': "{'title_ui': 'Etiologisk kod', 'description': 'Förtydligar diagnosen (ICD10, ICD10P eller ATC)'}", 'Cause1': "{'title_ui': 'Orsakskod 1', 'description': 'Orsakskod 1 (orsak till diagnosen, ICD10)'}", 'Cause2': "{'title_ui': 'Orsakskod 2', 'description': 'Orsakskod 2 (orsak till diagnosen, ICD10)'}", 'IsCauseOfDeath': "{'title_ui': 'Dödsorsak', 'description': 'Om denna diagnos var dödsorsaken'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Cause1] AS VARCHAR(MAX)) AS [Cause1],
		CAST([Cause2] AS VARCHAR(MAX)) AS [Cause2],
		CAST([DiagnosisCode] AS VARCHAR(MAX)) AS [DiagnosisCode],
		CAST([DiagnosisID] AS VARCHAR(MAX)) AS [DiagnosisID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([EtiologicDiagnosisID] AS VARCHAR(MAX)) AS [EtiologicDiagnosisID],
		CAST([IsCauseOfDeath] AS VARCHAR(MAX)) AS [IsCauseOfDeath],
		CAST([IsMainDiagnosis] AS VARCHAR(MAX)) AS [IsMainDiagnosis],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vPAS_DiagnosesDRG) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    