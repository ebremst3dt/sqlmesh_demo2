
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Uppgifter som enbart gäller vid inskrivning (vårdtillfälle)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdmissionID': 'varchar(max)', 'DischargeDatetime': 'varchar(max)', 'DischargeFormID': 'varchar(max)', 'DischargeID': 'varchar(max)', 'DischargeReadyDate': 'varchar(max)', 'DischargingCareUnitEXID': 'varchar(max)', 'DischargingCareUnitKombika': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EmergencyID': 'varchar(max)', 'Group': 'varchar(max)', 'HasEarlyRetirementPension': 'varchar(max)', 'IsExemptFromFees': 'varchar(max)', 'IsSecret': 'varchar(max)', 'PatientClassID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PaymentLiabilityDate': 'varchar(max)', 'PlannedDischargeDate': 'varchar(max)', 'ResponsibleDoctorUserID': 'varchar(max)', 'ResponsibleDoctorUserName': 'varchar(max)', 'ResponsibleNurseUserID': 'varchar(max)', 'ResponsibleNurseUserName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransportationNeed': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'EmergencyID': "{'title_ui': 'Akut', 'description': {'break': [None, None]}}", 'HasEarlyRetirementPension': "{'title_ui': 'Patientavgift', 'description': 'Om patienten är förtidspensionerad och under 40 år (får sjuk- eller aktivitetsersättning)'}", 'IsExemptFromFees': "{'title_ui': 'Patientavgift', 'description': 'Om patienten är avgiftsbefriad (ex. värnpliktiga, interner etc)'}", 'PatientClassID': "{'title_ui': 'Patientklass', 'description': 'Används inte längre'}", 'IsSecret': "{'title_ui': 'Upplysning får (ej) lämnas', 'description': 'Om upplysningar om patienten ej får lämnas ut under vårdtillfället'}", 'Group': "{'title_ui': 'Grupp', 'description': 'Fritext för att gruppera inskrivningar'}", 'ResponsibleNurseUserID': "{'title_ui': 'Ansvarig Ssk/Bmsk', 'description': 'Ansvarig sjuksköterska/barnmorska under vårdförloppet (fr.o.m. sommaren 2007, tidigare lagrades endast användarnamn)'}", 'ResponsibleNurseUserName': "{'title_ui': 'Ansvarig Ssk/Bmsk', 'description': None}", 'ResponsibleDoctorUserID': "{'title_ui': 'Ansvarig läkare', 'description': 'Ansvarig läkare under vårdförloppet (fr.o.m. sommaren 2007, tidigare lagrades endast användarnamn)'}", 'ResponsibleDoctorUserName': "{'title_ui': 'Ansvarig läkare', 'description': None}", 'AdmissionID': "{'title_ui': 'Inskrivningskod', 'description': 'Beror på vårdenhetens län'}", 'DischargeID': "{'title_ui': 'Utskrivningskod', 'description': 'Beror på vårdenhetens län'}", 'DischargeDatetime': "{'title_ui': 'Utskrivningstid', 'description': None}", 'DischargeFormID': "{'title_ui': 'Utskrivningsform', 'description': None}", 'DischargingCareUnitKombika': "{'title_ui': 'Ekonomisk enhet (ut)', 'description': 'De tre sista tecknen i aktuell ekonomisk kombika vid utskrivning. Samma som den senaste ekonomiska kombikan i PAS_Inpatient_Transfers.'}", 'DischargingCareUnitEXID': "{'title_ui': 'Ekonomisk enhet (ut)', 'description': 'Fullständig aktuell ekonomisk enhet vid utskrivning. Samma som den senaste ekonomiska enheten i PAS_Inpatient_Transfers.'}", 'DischargeReadyDate': "{'title_ui': 'Datum utskrivningsklar', 'description': 'Det datum patienten blivit utskrivningsklar (se också tabellen PAS_Inpatient_DischargeReady)'}", 'PaymentLiabilityDate': "{'title_ui': 'Betalningsansvarig datum', 'description': 'Det datum betalningsansvar övergår till kommunen vid samordnad vårdplanering'}", 'PlannedDischargeDate': "{'title_ui': 'Planerat utskrivningsdatum', 'description': 'Datum då patienten förväntas kunna skrivas ut (användes inte 2002-2010, men är nu åter aktivt). Startdatum för ärende lagrades felaktigt här våren 2010.'}", 'TransportationNeed': "{'title_ui': 'Transportbehov/Övrigt', 'description': 'En fritextruta med kommentar till utskrivaren'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID']
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
		CAST([AdmissionID] AS VARCHAR(MAX)) AS [AdmissionID],
		CONVERT(varchar(max), [DischargeDatetime], 126) AS [DischargeDatetime],
		CAST([DischargeFormID] AS VARCHAR(MAX)) AS [DischargeFormID],
		CAST([DischargeID] AS VARCHAR(MAX)) AS [DischargeID],
		CONVERT(varchar(max), [DischargeReadyDate], 126) AS [DischargeReadyDate],
		CAST([DischargingCareUnitEXID] AS VARCHAR(MAX)) AS [DischargingCareUnitEXID],
		CAST([DischargingCareUnitKombika] AS VARCHAR(MAX)) AS [DischargingCareUnitKombika],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([EmergencyID] AS VARCHAR(MAX)) AS [EmergencyID],
		CAST([Group] AS VARCHAR(MAX)) AS [Group],
		CAST([HasEarlyRetirementPension] AS VARCHAR(MAX)) AS [HasEarlyRetirementPension],
		CAST([IsExemptFromFees] AS VARCHAR(MAX)) AS [IsExemptFromFees],
		CAST([IsSecret] AS VARCHAR(MAX)) AS [IsSecret],
		CAST([PatientClassID] AS VARCHAR(MAX)) AS [PatientClassID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PaymentLiabilityDate], 126) AS [PaymentLiabilityDate],
		CONVERT(varchar(max), [PlannedDischargeDate], 126) AS [PlannedDischargeDate],
		CAST([ResponsibleDoctorUserID] AS VARCHAR(MAX)) AS [ResponsibleDoctorUserID],
		CAST([ResponsibleDoctorUserName] AS VARCHAR(MAX)) AS [ResponsibleDoctorUserName],
		CAST([ResponsibleNurseUserID] AS VARCHAR(MAX)) AS [ResponsibleNurseUserID],
		CAST([ResponsibleNurseUserName] AS VARCHAR(MAX)) AS [ResponsibleNurseUserName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransportationNeed] AS VARCHAR(MAX)) AS [TransportationNeed] 
	FROM Intelligence.viewreader.vPAS_Inpatient) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    