
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdministrationRoute': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'DiscontinuedDate': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Dosage': 'varchar(max)', 'DosageText': 'varchar(max)', 'DoseForm': 'varchar(max)', 'DrugNameText': 'varchar(max)', 'EnteredDate': 'varchar(max)', 'InfusionEndDate': 'varchar(max)', 'InfusionEndTime': 'varchar(max)', 'IsEnteredLongTimeAgo': 'varchar(max)', 'IsMissingLatestDoseDateTime': 'varchar(max)', 'LatestDoseDate': 'varchar(max)', 'LatestDoseTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationID': 'varchar(max)', 'Strength': 'varchar(max)', 'StrengthUnit': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)', 'TubeName': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': 'Substanser som ska analyseras', 'description': 'Kod för vald analys'}", 'Analysis': "{'title_ui': 'Substanser som ska analyseras', 'description': 'Vald analys/substans i klartext'}", 'PreparationID': "{'title_ui': None, 'description': 'Preparatets preparat-id/drugId (Apotekets/SILs interna ID)'}", 'DrugNameText': "{'title_ui': 'Läkemedel som ska analyseras', 'description': 'Preparat, fritextfält'}", 'DoseForm': "{'title_ui': 'Läkemedel som ska analyseras', 'description': 'Läkemedelsform'}", 'Strength': "{'title_ui': 'Läkemedel som ska analyseras', 'description': 'Preparatets styrka. Kunde anges för beställningar t.o.m. 2004, efter det alltid 1.'}", 'StrengthUnit': "{'title_ui': 'Dosering', 'description': 'Preparatets styrkeenhet'}", 'Dosage': "{'title_ui': 'Dosering', 'description': 'Dos'}", 'DosageText': "{'title_ui': 'Dosering', 'description': 'Dos i klartext'}", 'AdministrationRoute': "{'title_ui': 'Adm. sätt', 'description': 'Administrationsväg'}", 'EnteredDate': "{'title_ui': 'Insatt', 'description': 'Insatt datum'}", 'DiscontinuedDate': "{'title_ui': 'Utsatt', 'description': 'Utsatt datum'}", 'IsEnteredLongTimeAgo': "{'title_ui': 'Insatt s.länge', 'description': None}", 'LatestDoseDate': "{'title_ui': 'Senaste dos/Inf påbörjad', 'description': 'Datum för senaste dos eller infusion påbörjad'}", 'LatestDoseTime': "{'title_ui': 'Senaste dos/Inf påbörjad', 'description': 'Tiden för senaste dos eller infusion påbörjad'}", 'IsMissingLatestDoseDateTime': '{\'title_ui\': \'Senaste dos - uppgift saknas\', \'description\': \'Sätts om kryssrytan "Senaste dos - uppgift saknas" är vald.\'}', 'InfusionEndDate': "{'title_ui': 'Inf avslutad', 'description': 'Fältet visas då läkemedelsform är INF'}", 'InfusionEndTime': "{'title_ui': 'Inf avslutad', 'description': 'Fältet visas då läkemedelsform är INF'}", 'TubeID': "{'title_ui': 'Rörnamn', 'description': 'Rörkod'}", 'TubeName': "{'title_ui': 'Rörnamn', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AdministrationRoute] AS VARCHAR(MAX)) AS [AdministrationRoute],
		CAST([Analysis] AS VARCHAR(MAX)) AS [Analysis],
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CONVERT(varchar(max), [DiscontinuedDate], 126) AS [DiscontinuedDate],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Dosage] AS VARCHAR(MAX)) AS [Dosage],
		CAST([DosageText] AS VARCHAR(MAX)) AS [DosageText],
		CAST([DoseForm] AS VARCHAR(MAX)) AS [DoseForm],
		CAST([DrugNameText] AS VARCHAR(MAX)) AS [DrugNameText],
		CONVERT(varchar(max), [EnteredDate], 126) AS [EnteredDate],
		CONVERT(varchar(max), [InfusionEndDate], 126) AS [InfusionEndDate],
		CONVERT(varchar(max), [InfusionEndTime], 126) AS [InfusionEndTime],
		CAST([IsEnteredLongTimeAgo] AS VARCHAR(MAX)) AS [IsEnteredLongTimeAgo],
		CAST([IsMissingLatestDoseDateTime] AS VARCHAR(MAX)) AS [IsMissingLatestDoseDateTime],
		CONVERT(varchar(max), [LatestDoseDate], 126) AS [LatestDoseDate],
		CONVERT(varchar(max), [LatestDoseTime], 126) AS [LatestDoseTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PreparationID] AS VARCHAR(MAX)) AS [PreparationID],
		CAST([Strength] AS VARCHAR(MAX)) AS [Strength],
		CAST([StrengthUnit] AS VARCHAR(MAX)) AS [StrengthUnit],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TubeID] AS VARCHAR(MAX)) AS [TubeID],
		CAST([TubeName] AS VARCHAR(MAX)) AS [TubeName],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vPharmacologyOrders_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    