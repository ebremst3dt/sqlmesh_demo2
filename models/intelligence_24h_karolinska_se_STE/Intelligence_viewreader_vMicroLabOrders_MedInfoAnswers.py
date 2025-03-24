
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Det användaren skriver in i kompletterande uppgifter (tidigare medicinsk information). Innehålla en rad per rad i Kompl. uppgifter för de flesta analyser med några få analyser kan ha flera rader.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Answer': 'varchar(max)', 'DocumentID': 'varchar(max)', 'MedInfoRow': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'MedInfoRow': "{'title_ui': None, 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Answer': "{'title_ui': None, 'description': 'Svar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Answer] AS VARCHAR(MAX)) AS [Answer],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([MedInfoRow] AS VARCHAR(MAX)) AS [MedInfoRow],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMicroLabOrders_MedInfoAnswers) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    