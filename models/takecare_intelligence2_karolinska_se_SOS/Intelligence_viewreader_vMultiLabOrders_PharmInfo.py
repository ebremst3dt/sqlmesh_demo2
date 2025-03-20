
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Valda analyser farmakologi.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdministrationRouteCode': 'varchar(max)', 'DiscontinuedDate': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Dosage': 'varchar(max)', 'DosageText': 'varchar(max)', 'DoseForm': 'varchar(max)', 'DrugNameText': 'varchar(max)', 'EnteredDate': 'varchar(max)', 'InfusionEndDateTime': 'varchar(max)', 'InfusionStartDateTime': 'varchar(max)', 'IsEnteredLongTimeAgo': 'varchar(max)', 'IsMissingLatestDoseDateTime': 'varchar(max)', 'LatestDoseDateTime': 'varchar(max)', 'OrderableID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationID': 'varchar(max)', 'Row': 'varchar(max)', 'Strength': 'varchar(max)', 'StrengthUnit': 'varchar(max)', 'SubstancePreparation': 'varchar(max)', 'SubstancePreparationID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TubeID': 'varchar(max)', 'TubeName': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'OrderableID': "{'title_ui': None, 'description': 'Kod för vald beställningsspec. Dvs. beställningsbar kombination av analys, undersökning, rör och provmaterial'}", 'SubstancePreparationID': "{'title_ui': 'Substans', 'description': 'Kod för vald analys'}", 'SubstancePreparation': "{'title_ui': 'Substans', 'description': 'Vald substans och materiel, visas endast i beställningsbilden/läkemedelsinformation'}", 'PreparationID': "{'title_ui': None, 'description': 'Preparatets preparat-id/drugId (Apotekets/SILs interna ID)'}", 'DrugNameText': "{'title_ui': 'Läkemedel som ska analyseras', 'description': 'Preparat, fritextfält'}", 'DoseForm': "{'title_ui': 'Läkemedel som ska analyseras', 'description': 'Läkemedelsform'}", 'Strength': "{'title_ui': None, 'description': 'Preparatets styrka. Alltid 1'}", 'StrengthUnit': "{'title_ui': 'Dosering', 'description': 'Preparatets styrkeenhet'}", 'Dosage': "{'title_ui': 'Dosering', 'description': 'Dos'}", 'DosageText': "{'title_ui': 'Dosering', 'description': 'Dos i klartext'}", 'AdministrationRouteCode': "{'title_ui': 'Adm. sätt', 'description': 'Administrationsväg'}", 'EnteredDate': "{'title_ui': 'Insatt', 'description': 'Insatt datum'}", 'DiscontinuedDate': "{'title_ui': 'Utsatt', 'description': 'Utsatt datum'}", 'IsEnteredLongTimeAgo': "{'title_ui': 'Insatt s.länge', 'description': None}", 'LatestDoseDateTime': "{'title_ui': 'Senaste dos', 'description': 'Senaste dos'}", 'IsMissingLatestDoseDateTime': "{'title_ui': 'Senaste dos - uppgift saknas', 'description': None}", 'InfusionStartDateTime': "{'title_ui': 'Inf påbörjad', 'description': 'Fältet visas då läkemedelsform är INF'}", 'InfusionEndDateTime': "{'title_ui': 'Inf avslutad', 'description': 'Fältet visas då läkemedelsform är INF'}", 'TubeID': "{'title_ui': 'Material/Materiel', 'description': 'Rörkod'}", 'TubeName': "{'title_ui': 'Material/Materiel', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AdministrationRouteCode AS VARCHAR(MAX)) AS AdministrationRouteCode,
		CONVERT(varchar(max), DiscontinuedDate, 126) AS DiscontinuedDate,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Dosage AS VARCHAR(MAX)) AS Dosage,
		CAST(DosageText AS VARCHAR(MAX)) AS DosageText,
		CAST(DoseForm AS VARCHAR(MAX)) AS DoseForm,
		CAST(DrugNameText AS VARCHAR(MAX)) AS DrugNameText,
		CONVERT(varchar(max), EnteredDate, 126) AS EnteredDate,
		CONVERT(varchar(max), InfusionEndDateTime, 126) AS InfusionEndDateTime,
		CONVERT(varchar(max), InfusionStartDateTime, 126) AS InfusionStartDateTime,
		CAST(IsEnteredLongTimeAgo AS VARCHAR(MAX)) AS IsEnteredLongTimeAgo,
		CAST(IsMissingLatestDoseDateTime AS VARCHAR(MAX)) AS IsMissingLatestDoseDateTime,
		CONVERT(varchar(max), LatestDoseDateTime, 126) AS LatestDoseDateTime,
		CAST(OrderableID AS VARCHAR(MAX)) AS OrderableID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PreparationID AS VARCHAR(MAX)) AS PreparationID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(Strength AS VARCHAR(MAX)) AS Strength,
		CAST(StrengthUnit AS VARCHAR(MAX)) AS StrengthUnit,
		CAST(SubstancePreparation AS VARCHAR(MAX)) AS SubstancePreparation,
		CAST(SubstancePreparationID AS VARCHAR(MAX)) AS SubstancePreparationID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TubeID AS VARCHAR(MAX)) AS TubeID,
		CAST(TubeName AS VARCHAR(MAX)) AS TubeName,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMultiLabOrders_PharmInfo) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    