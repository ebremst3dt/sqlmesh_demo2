
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Ordinationsdata",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdministrationOccasionID': 'varchar(max)', 'AdministrationRouteID': 'varchar(max)', 'AdministrationTypeID': 'varchar(max)', 'CessationReasonID': 'varchar(max)', 'DilutionAmount': 'varchar(max)', 'DilutionLiquid': 'varchar(max)', 'DocumentID': 'varchar(max)', 'FirstDoseDate': 'varchar(max)', 'FirstDoseTime': 'varchar(max)', 'Instruction': 'varchar(max)', 'IsDispensionAllowed': 'varchar(max)', 'IsReplaceable': 'varchar(max)', 'IsStdSolution': 'varchar(max)', 'LastDoseDate': 'varchar(max)', 'LastDoseTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationInstruction': 'varchar(max)', 'ReviewDate': 'varchar(max)', 'ReviewDecisionByUserID': 'varchar(max)', 'ReviewTime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SolutionStrength': 'varchar(max)', 'SolutionStrengthUnitID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TreatmentGoal': 'varchar(max)', 'TreatmentReason': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': 'Version', 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Användaren som sparat ordinationsdata'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Där ordinationsdata är sparat'}", 'AdministrationRouteID': '{\'title_ui\': \'Administrationsväg\', \'description\': \'0 betyder "Annat"\'}', 'AdministrationTypeID': '{\'title_ui\': \'Administrationsmetod\', \'description\': \'0 betyder "Annat"\'}', 'TreatmentReason': "{'title_ui': 'Behandlingsorsak', 'description': None}", 'TreatmentGoal': "{'title_ui': 'Behandlingsmål', 'description': None}", 'Instruction': "{'title_ui': 'Instruktion för administrering', 'description': 'Instruktion/kommentar för administrering'}", 'PreparationInstruction': "{'title_ui': 'Instruktion för iordningställande', 'description': 'Instruktion/kommentar för iordningställande'}", 'ReviewDate': "{'title_ui': 'Nytt ställningstagande datum', 'description': 'Datum då nytt ställningstagande till ordinationen måste ske (senast)'}", 'ReviewTime': "{'title_ui': 'Nytt ställningstagande datum', 'description': 'Klockslag då nytt ställningstagande till ordinationen måste ske (senast)'}", 'ReviewDecisionByUserID': "{'title_ui': 'Nytt ställningstagande namn', 'description': 'Den som angett datum för nytt ställningstagande'}", 'IsReplaceable': "{'title_ui': 'Får (ej) bytas ut', 'description': 'Sant om preparaten får bytas ut mot generika'}", 'DilutionLiquid': "{'title_ui': 'Spädes i', 'description': 'Det preparat man späder med'}", 'DilutionAmount': "{'title_ui': 'Spädningsvolym', 'description': 'I ml'}", 'CessationReasonID': "{'title_ui': 'Utsättningsorsak', 'description': None}", 'IsStdSolution': "{'title_ui': 'Lösning', 'description': 'Om preparaten ska blandas till en lösning/stamlösning'}", 'FirstDoseDate': "{'title_ui': 'Ord gäller fr.o.m.', 'description': 'När ordinationen sätts in (första dos-datum)'}", 'FirstDoseTime': "{'title_ui': 'Ord gäller fr.o.m.', 'description': None}", 'LastDoseDate': "{'title_ui': 'Ord gäller t.o.m.', 'description': 'När ordinationen sätts ut (sista dos-datum)'}", 'LastDoseTime': "{'title_ui': 'Ord gäller t.o.m.', 'description': None}", 'AdministrationOccasionID': "{'title_ui': 'Administrationstillfälle', 'description': {'break': None}}", 'IsDispensionAllowed': "{'title_ui': 'Får (ej) dosdispenseras', 'description': 'Falskt om ordinationen ej får dosdispenseras av Apoteket. För enheter där det ej går att dosdispensera, blir det alltid sant. Ny 2007-11.'}", 'SolutionStrength': "{'title_ui': 'Styrka bruksfärdig lösning', 'description': 'Styrka på stamlösning'}", 'SolutionStrengthUnitID': "{'title_ui': 'Styrka bruksfärdig lösning', 'description': 'Enhet för styrka på stamlösning'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([AdministrationOccasionID] AS VARCHAR(MAX)) AS [AdministrationOccasionID],
		CAST([AdministrationRouteID] AS VARCHAR(MAX)) AS [AdministrationRouteID],
		CAST([AdministrationTypeID] AS VARCHAR(MAX)) AS [AdministrationTypeID],
		CAST([CessationReasonID] AS VARCHAR(MAX)) AS [CessationReasonID],
		CAST([DilutionAmount] AS VARCHAR(MAX)) AS [DilutionAmount],
		CAST([DilutionLiquid] AS VARCHAR(MAX)) AS [DilutionLiquid],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [FirstDoseDate], 126) AS [FirstDoseDate],
		CONVERT(varchar(max), [FirstDoseTime], 126) AS [FirstDoseTime],
		CAST([Instruction] AS VARCHAR(MAX)) AS [Instruction],
		CAST([IsDispensionAllowed] AS VARCHAR(MAX)) AS [IsDispensionAllowed],
		CAST([IsReplaceable] AS VARCHAR(MAX)) AS [IsReplaceable],
		CAST([IsStdSolution] AS VARCHAR(MAX)) AS [IsStdSolution],
		CONVERT(varchar(max), [LastDoseDate], 126) AS [LastDoseDate],
		CONVERT(varchar(max), [LastDoseTime], 126) AS [LastDoseTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PreparationInstruction] AS VARCHAR(MAX)) AS [PreparationInstruction],
		CONVERT(varchar(max), [ReviewDate], 126) AS [ReviewDate],
		CAST([ReviewDecisionByUserID] AS VARCHAR(MAX)) AS [ReviewDecisionByUserID],
		CONVERT(varchar(max), [ReviewTime], 126) AS [ReviewTime],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SolutionStrength] AS VARCHAR(MAX)) AS [SolutionStrength],
		CAST([SolutionStrengthUnitID] AS VARCHAR(MAX)) AS [SolutionStrengthUnitID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([TreatmentGoal] AS VARCHAR(MAX)) AS [TreatmentGoal],
		CAST([TreatmentReason] AS VARCHAR(MAX)) AS [TreatmentReason],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMedOrders_Prescription) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    