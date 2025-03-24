
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Diagnoser som användaren kan söka efter i diagnosregister, primärvårdsdiagnoser, orsaksregister eller ATC-register. Maximalt fyra diagnosrader kan tillhöra en diagnosversion.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Code': 'varchar(max)', 'CodeTable': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Text': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Type': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'Code': '{\'title_ui\': \'Kod/Etiologi/Orsak\', \'description\': \'Diagnoskod (ICD10, ICD10P eller ATC). Primärvårdsdiagnoskoder var från början felaktigt laddade, utan tecknet "-". Felaktiga koder kan ligga kvar.\'}', 'Text': "{'title_ui': 'Diagnos/Etiologi/Orsak', 'description': 'Diagnostext (fördefinierad från ICD10, ICD10P eller ATC). Texten kan dock ändras av användaren. Kolumn Code ger tillhörande kod.'}", 'CodeTable': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Code] AS VARCHAR(MAX)) AS [Code],
		CAST([CodeTable] AS VARCHAR(MAX)) AS [CodeTable],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Text] AS VARCHAR(MAX)) AS [Text],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Type] AS VARCHAR(MAX)) AS [Type],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vDiagnoses_RegistryCodes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    