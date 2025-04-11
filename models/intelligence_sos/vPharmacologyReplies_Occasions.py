
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Provtagningstillfällen.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'GroupRow': 'varchar(max)', 'HoursSinceLatestDose': 'varchar(max)', 'LID': 'varchar(max)', 'OccasionRow': 'varchar(max)', 'PatientID': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TreatmentTime': 'varchar(max)', 'Unit24HourDose': 'varchar(max)', 'Value24HourDose': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'GroupRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'OccasionRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'LID': "{'title_ui': None, 'description': 'Labb-id, dvs. labbsvarets id i laboratoriets system'}", 'SamplingDate': "{'title_ui': 'Provtagningsdatum', 'description': 'Datum då provtagning skett'}", 'SamplingTime': "{'title_ui': 'Tid', 'description': 'Klockslag då provtagning skett'}", 'Value24HourDose': "{'title_ui': 'Dygnsdos', 'description': None}", 'Unit24HourDose': "{'title_ui': 'Enhet', 'description': 'Enhet för dygnsdos. Visas inte alltid i TakeCare.'}", 'HoursSinceLatestDose': "{'title_ui': 'Tid från dos', 'description': 'Tid från senaste dos i timmar'}", 'TreatmentTime': "{'title_ui': 'Behandlingstid', 'description': 'Behandlingstid i dagar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'GroupRow', 'OccasionRow', 'PatientID', 'Version']
    ),
    cron="@daily",
    start=start,
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
		CAST([GroupRow] AS VARCHAR(MAX)) AS [GroupRow],
		CAST([HoursSinceLatestDose] AS VARCHAR(MAX)) AS [HoursSinceLatestDose],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([OccasionRow] AS VARCHAR(MAX)) AS [OccasionRow],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TreatmentTime] AS VARCHAR(MAX)) AS [TreatmentTime],
		CAST([Unit24HourDose] AS VARCHAR(MAX)) AS [Unit24HourDose],
		CAST([Value24HourDose] AS VARCHAR(MAX)) AS [Value24HourDose],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vPharmacologyReplies_Occasions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    