
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Även avslutade infusioner registreras som en administrering, men historiken följer inte med. Observera att lösningar och vid behovs blandningar endast lagras som en rad i denna tabell.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdministrationDatetime': 'varchar(max)', 'AsNeededNo': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Dose': 'varchar(max)', 'InfusionKey': 'varchar(max)', 'OrderCreatedAtCareUnitID': 'varchar(max)', 'OrderDoseText': 'varchar(max)', 'OrderDoseTextSolution': 'varchar(max)', 'OxygenTreatmentKey': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationNo': 'varchar(max)', 'PrescriptionDate': 'varchar(max)', 'PrescriptionTime': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TreatmentReason': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'TimestampSaved': "{'title_ui': None, 'description': 'Tidpunkt då denna administrering sparades'}", 'SavedByUserID': "{'title_ui': None, 'description': 'Användaren som sparat data'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Där data är sparat'}", 'AsNeededNo': "{'title_ui': None, 'description': 'Löpnummer som identifierar återkommande vid behovs-administreringar. Är samma för alla versioner av samma administrering.'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar till administrering'}", 'TreatmentReason': "{'title_ui': 'Behandlingsorsak', 'description': 'Varför patienten administrerades preparatet'}", 'AdministrationDatetime': "{'title_ui': 'Administrerad/Adm.datum, Adm.tid', 'description': 'Händelsetid för administrering, dvs då administrering uppges ha skett'}", 'PrescriptionDate': "{'title_ui': None, 'description': 'Det datum som administreringen avser. Används för att koppla ihop med doseringsanvisningar.'}", 'PrescriptionTime': "{'title_ui': None, 'description': 'Det klockslag som administreringen avser. Används för att koppla ihop med doseringsanvisningar. Är NULL om vid behov.'}", 'InfusionKey': "{'title_ui': None, 'description': 'Tidsstämpel för koppling från infusionsdata'}", 'OrderCreatedAtCareUnitID': "{'title_ui': None, 'description': 'Används för att avgöra vilken vårdenhet som har behörighet att se administreringen'}", 'OrderDoseText': "{'title_ui': 'Ord dos', 'description': 'Ordinerad dos/mängd med enhet för samtliga preparat i ordinationen. Ny fr.o.m. version 12.6.'}", 'OrderDoseTextSolution': "{'title_ui': 'Ord dos', 'description': 'Ordinerad dos av bruksfärdig lösning. Ny fr.o.m. version 12.6.'}", 'PreparationNo': "{'title_ui': None, 'description': 'ID för preparatet i listan (löpnummer)'}", 'Dose': "{'title_ui': 'Adm dos', 'description': 'Administrerad dos. Kan innehålla text eller flera numeriska värden separerade med mellanslag då det är flera preparat i samma ordination. Före v. 12.6 kan mellanslag felaktigt saknas.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), AdministrationDatetime, 126) AS AdministrationDatetime,
		CAST(AsNeededNo AS VARCHAR(MAX)) AS AsNeededNo,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Dose AS VARCHAR(MAX)) AS Dose,
		CONVERT(varchar(max), InfusionKey, 126) AS InfusionKey,
		CAST(OrderCreatedAtCareUnitID AS VARCHAR(MAX)) AS OrderCreatedAtCareUnitID,
		CAST(OrderDoseText AS VARCHAR(MAX)) AS OrderDoseText,
		CAST(OrderDoseTextSolution AS VARCHAR(MAX)) AS OrderDoseTextSolution,
		CONVERT(varchar(max), OxygenTreatmentKey, 126) AS OxygenTreatmentKey,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PreparationNo AS VARCHAR(MAX)) AS PreparationNo,
		CONVERT(varchar(max), PrescriptionDate, 126) AS PrescriptionDate,
		CONVERT(varchar(max), PrescriptionTime, 126) AS PrescriptionTime,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(TreatmentReason AS VARCHAR(MAX)) AS TreatmentReason 
	FROM Intelligence.viewreader.vMedOrders_Administration) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    