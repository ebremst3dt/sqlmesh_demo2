
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="PAS-post. Patientadministrativt system (in- och utskrivningar, diagnos- och åtgärdsregistrering, kassatransaktioner mm). PAS-poster versionshanteras inte i TakeCare.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CarePlanningID': 'varchar(max)', 'CaseID': 'varchar(max)', 'CaseStartDatetime': 'varchar(max)', 'Comment': 'varchar(max)', 'ContactResourceID': 'varchar(max)', 'ContactTypeCode': 'varchar(max)', 'DRGKOKSApprovedByUserName': 'varchar(max)', 'DiagnosisApprovedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EconomicalInKombika': 'varchar(max)', 'IdentificationTypeID': 'varchar(max)', 'IsInpatient': 'varchar(max)', 'IsSLLPatient': 'varchar(max)', 'PASType': 'varchar(max)', 'PatientCountyID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PatientMunicipalityID': 'varchar(max)', 'ReferredToKombika': 'varchar(max)', 'ReferringKombika': 'varchar(max)', 'RegistrationStatus': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'StartDatetime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'När data sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Den vårdenhet där PAS-posten är registrerad. Vid inskrivning är det vårdenhetens län som ev. styr kodtabeller.'}", 'RegistrationStatus': "{'title_ui': 'Registreringsstatus', 'description': {'break': [None, None, None, None, None, None, None]}}", 'DiagnosisApprovedByUserID': "{'title_ui': None, 'description': 'Användaren som godkänt vårdkontakten, genom att välja knappen Godkänn. Det har funnits ett fel som satte värdet även vid valet OK fram till 20100420.'}", 'DRGKOKSApprovedByUserName': "{'title_ui': None, 'description': 'Användaren som godkänt vårdkontakten, genom att välja knappen Godkänn. Det har funnits ett fel som satte värdet även vid valet OK fram till 20100420.'}", 'IsInpatient': "{'title_ui': None, 'description': 'Om posten gäller slutenvård (vårdtillfälle). Annars öppenvård (vårdkontakt).'}", 'PASType': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'ContactTypeCode': "{'title_ui': 'Kontakttyp', 'description': 'Kod för kontakttyp'}", 'CaseID': "{'title_ui': 'GVR-nyckel', 'description': 'ÄrendeID'}", 'ContactResourceID': "{'title_ui': 'Resurs', 'description': 'Resurs för denna kontakt'}", 'EconomicalInKombika': "{'title_ui': 'Ekonomisk enhet', 'description': 'Den ekonomiska enheten, antingen vid inskrivning eller i kassan. Kombika/EXID.'}", 'PatientCountyID': "{'title_ui': 'Län', 'description': 'Patientens hemlän (vid detta tillfälle). Hämtas från patientuppgifter, som hämtas från CFU.'}", 'PatientMunicipalityID': "{'title_ui': None, 'description': 'Patientens hemkommun (vid detta tillfälle). Hämtas från patientuppgifter, som hämtas från CFU.'}", 'IdentificationTypeID': "{'title_ui': 'Identifikationssätt', 'description': 'Det sätt patienten identifierats på'}", 'StartDatetime': "{'title_ui': 'Inskrivningsdatum/Datum+Ankomst', 'description': 'Tidpunkt för besök eller inskrivning, eller registreringsdatum för ärenden. Om patienten redan har ett ärende registrerat denna dag lagras första lediga datum bakåt i tiden.'}", 'CaseStartDatetime': "{'title_ui': 'Startdatum', 'description': 'Startdatum för ärenden'}", 'ReferringKombika': "{'title_ui': 'Remitterande vårdenhet', 'description': 'Kombika/EXID från vilken patienten anlänt. Kan också innehålla andra koder, ex. EJSLL, OKÄND och ÖVRIG.'}", 'ReferredToKombika': "{'title_ui': 'Remitterad/Utskriven till', 'description': 'Kombika/EXID till vilken patienten skickats. Kan också innehålla andra koder, ex. EJSLL, OKÄND och ÖVRIG.'}", 'IsSLLPatient': '{\'title_ui\': None, \'description\': \'Om patienten är "avtalsmässigt SLL-patient" (används endast av Stockholms sjukhem, 0 för alla andra)\'}', 'CarePlanningID': "{'title_ui': 'Vårdplaneringskod SLL', 'description': 'Anges i DRG/KÖKS för öppenvårdskontakter'}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CarePlanningID AS VARCHAR(MAX)) AS CarePlanningID,
		CAST(CaseID AS VARCHAR(MAX)) AS CaseID,
		CONVERT(varchar(max), CaseStartDatetime, 126) AS CaseStartDatetime,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(ContactResourceID AS VARCHAR(MAX)) AS ContactResourceID,
		CAST(ContactTypeCode AS VARCHAR(MAX)) AS ContactTypeCode,
		CAST(DRGKOKSApprovedByUserName AS VARCHAR(MAX)) AS DRGKOKSApprovedByUserName,
		CAST(DiagnosisApprovedByUserID AS VARCHAR(MAX)) AS DiagnosisApprovedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(EconomicalInKombika AS VARCHAR(MAX)) AS EconomicalInKombika,
		CAST(IdentificationTypeID AS VARCHAR(MAX)) AS IdentificationTypeID,
		CAST(IsInpatient AS VARCHAR(MAX)) AS IsInpatient,
		CAST(IsSLLPatient AS VARCHAR(MAX)) AS IsSLLPatient,
		CAST(PASType AS VARCHAR(MAX)) AS PASType,
		CAST(PatientCountyID AS VARCHAR(MAX)) AS PatientCountyID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PatientMunicipalityID AS VARCHAR(MAX)) AS PatientMunicipalityID,
		CAST(ReferredToKombika AS VARCHAR(MAX)) AS ReferredToKombika,
		CAST(ReferringKombika AS VARCHAR(MAX)) AS ReferringKombika,
		CAST(RegistrationStatus AS VARCHAR(MAX)) AS RegistrationStatus,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), StartDatetime, 126) AS StartDatetime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved 
	FROM Intelligence.viewreader.vPAS) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    