
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Varje tillfälle och läkemedel hamnar på en egen rad. En dosering vid ett tillfälle, bestående av flera läkemedel, lagras alltså som flera rader.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DosageID': 'varchar(max)', 'DoseNumerical': 'varchar(max)', 'DoseText': 'varchar(max)', 'DrugCode': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationNo': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': 'Version', 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'DosageID': "{'title_ui': None, 'description': None}", 'PreparationNo': "{'title_ui': None, 'description': 'ID för preparatet i listan (löpnummer)'}", 'DrugCode': "{'title_ui': None, 'description': 'Kod för preparatet'}", 'DoseText': "{'title_ui': 'Dos fritext', 'description': 'Dos inklusive enhet, eller fritext för vid behov-ordinationer.'}", 'DoseNumerical': "{'title_ui': 'Dos numerisk', 'description': 'Dos. Är null ex. om det är en vid behov-ordination eller behandlingsschema.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([DosageID] AS VARCHAR(MAX)) AS [DosageID],
		CAST([DoseNumerical] AS VARCHAR(MAX)) AS [DoseNumerical],
		CAST([DoseText] AS VARCHAR(MAX)) AS [DoseText],
		CAST([DrugCode] AS VARCHAR(MAX)) AS [DrugCode],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PreparationNo] AS VARCHAR(MAX)) AS [PreparationNo],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMedOrders_DosageDrugs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    