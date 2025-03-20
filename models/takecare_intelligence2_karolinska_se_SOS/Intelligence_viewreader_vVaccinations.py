
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Vaccinationer",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdministeredAtCareUnitID': 'varchar(max)', 'AdministeredAtCareUnitName': 'varchar(max)', 'AdministeredByName': 'varchar(max)', 'AdministeredByUserID': 'varchar(max)', 'AdministrationComment': 'varchar(max)', 'AdministrationDate': 'varchar(max)', 'AdministrationDateText': 'varchar(max)', 'AdministrationRouteID': 'varchar(max)', 'AdministrationTime': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DosageAmount': 'varchar(max)', 'DosageText': 'varchar(max)', 'DosageUnitID': 'varchar(max)', 'DoseNumber': 'varchar(max)', 'HealthDeclarationDocumentID': 'varchar(max)', 'Instructions': 'varchar(max)', 'PatientID': 'varchar(max)', 'PlannedDate': 'varchar(max)', 'PrescriberName': 'varchar(max)', 'PrescriberUserID': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByRoleID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'StatusID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'VaccinationLocalizationID': 'varchar(max)', 'VaccinationNotTakenCauseID': 'varchar(max)', 'VaccinationScheduleID': 'varchar(max)', 'VaccinationScheduleName': 'varchar(max)', 'VaccineBatchNumber': 'varchar(max)', 'VaccineDatabaseID': 'varchar(max)', 'VaccineName': 'varchar(max)', 'VaccineSpecialityID': 'varchar(max)', 'VaccineTypeID': 'varchar(max)', 'VaccineTypeName': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedByRoleID': "{'title_ui': None, 'description': 'Roll-id på användaren som sparat dokumentet'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'Tidpunkt för skapandet av första versionen'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Användaren som skapade den första versionen'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'StatusID': "{'title_ui': 'Status-id', 'description': {'break': [None, None, None, None, None]}}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': None}", 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': None}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'SavedAtCareUnitID': "{'title_ui': 'Senast sparad på', 'description': None}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'VaccineTypeID': "{'title_ui': 'Vaccintyp-id', 'description': 'Id på vaccintyp eller tomt vid endast fritext'}", 'VaccineTypeName': "{'title_ui': 'Vaccintyp', 'description': 'Namn på vaccintyp. Fritext vid status Registrerad'}", 'DoseNumber': "{'title_ui': 'Dos nr', 'description': 'Ordningstal för vilken dos vaccinationen gäller'}", 'PlannedDate': "{'title_ui': 'Planerat datum', 'description': None}", 'VaccineSpecialityID': "{'title_ui': 'Specid', 'description': 'Tomt vid fritext'}", 'VaccineDatabaseID': "{'title_ui': 'Databas-id', 'description': 'Tomt vid fritext'}", 'VaccineName': "{'title_ui': 'Preparat', 'description': 'Namn på preparatet. Fritext vid status Registrerad'}", 'DosageText': "{'title_ui': 'Dosering', 'description': 'Doseringsmängd + doseringsenhet'}", 'DosageAmount': "{'title_ui': 'Doseringsmängd', 'description': None}", 'DosageUnitID': "{'title_ui': 'Doseringsenhet', 'description': None}", 'VaccineBatchNumber': "{'title_ui': 'Batchnummer', 'description': 'Batchnummer som hittas på förpackningen för preparatet'}", 'PrescriberUserID': "{'title_ui': 'Ordinatör', 'description': None}", 'PrescriberName': "{'title_ui': 'Ordinatör', 'description': None}", 'Instructions': "{'title_ui': 'Instruktioner', 'description': 'Instruktioner för den som skall administrera'}", 'VaccinationScheduleName': "{'title_ui': 'Vaccinationsschema', 'description': 'Tomt för ordinationer utan schema'}", 'VaccinationScheduleID': "{'title_ui': 'Vaccinationsschema-id', 'description': None}", 'HealthDeclarationDocumentID': "{'title_ui': 'Hälsodeklaration', 'description': None}", 'AdministrationDate': "{'title_ui': 'Administrationsdatum', 'description': 'Datum för administration'}", 'AdministrationTime': "{'title_ui': 'Administrationsdatum', 'description': 'Klockslag för administration'}", 'AdministrationDateText': "{'title_ui': 'Administrationsdatum', 'description': 'Fritext vid status Registrering'}", 'AdministeredByUserID': "{'title_ui': 'Administrerad av', 'description': None}", 'AdministeredByName': "{'title_ui': 'Administrerad av', 'description': 'Namn på användare eller fritext vid status Registrerad'}", 'AdministeredAtCareUnitID': "{'title_ui': 'Administrerad vid', 'description': None}", 'AdministeredAtCareUnitName': "{'title_ui': 'Administrerad vid', 'description': 'Namn på vårdenhet eller fritext vid status Registrerad'}", 'AdministrationRouteID': "{'title_ui': 'Administrationsväg', 'description': None}", 'VaccinationLocalizationID': "{'title_ui': 'Lokalisation', 'description': 'Plats som vaccinet administeras'}", 'AdministrationComment': "{'title_ui': 'Administrationskommentar', 'description': None}", 'VaccinationNotTakenCauseID': "{'title_ui': 'Orsak ej utförd', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AdministeredAtCareUnitID AS VARCHAR(MAX)) AS AdministeredAtCareUnitID,
		CAST(AdministeredAtCareUnitName AS VARCHAR(MAX)) AS AdministeredAtCareUnitName,
		CAST(AdministeredByName AS VARCHAR(MAX)) AS AdministeredByName,
		CAST(AdministeredByUserID AS VARCHAR(MAX)) AS AdministeredByUserID,
		CAST(AdministrationComment AS VARCHAR(MAX)) AS AdministrationComment,
		CONVERT(varchar(max), AdministrationDate, 126) AS AdministrationDate,
		CAST(AdministrationDateText AS VARCHAR(MAX)) AS AdministrationDateText,
		CAST(AdministrationRouteID AS VARCHAR(MAX)) AS AdministrationRouteID,
		CONVERT(varchar(max), AdministrationTime, 126) AS AdministrationTime,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(DosageAmount AS VARCHAR(MAX)) AS DosageAmount,
		CAST(DosageText AS VARCHAR(MAX)) AS DosageText,
		CAST(DosageUnitID AS VARCHAR(MAX)) AS DosageUnitID,
		CAST(DoseNumber AS VARCHAR(MAX)) AS DoseNumber,
		CAST(HealthDeclarationDocumentID AS VARCHAR(MAX)) AS HealthDeclarationDocumentID,
		CAST(Instructions AS VARCHAR(MAX)) AS Instructions,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), PlannedDate, 126) AS PlannedDate,
		CAST(PrescriberName AS VARCHAR(MAX)) AS PrescriberName,
		CAST(PrescriberUserID AS VARCHAR(MAX)) AS PrescriberUserID,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByRoleID AS VARCHAR(MAX)) AS SavedByRoleID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CONVERT(varchar(max), SignedDatetime, 126) AS SignedDatetime,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CAST(StatusID AS VARCHAR(MAX)) AS StatusID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(VaccinationLocalizationID AS VARCHAR(MAX)) AS VaccinationLocalizationID,
		CAST(VaccinationNotTakenCauseID AS VARCHAR(MAX)) AS VaccinationNotTakenCauseID,
		CAST(VaccinationScheduleID AS VARCHAR(MAX)) AS VaccinationScheduleID,
		CAST(VaccinationScheduleName AS VARCHAR(MAX)) AS VaccinationScheduleName,
		CAST(VaccineBatchNumber AS VARCHAR(MAX)) AS VaccineBatchNumber,
		CAST(VaccineDatabaseID AS VARCHAR(MAX)) AS VaccineDatabaseID,
		CAST(VaccineName AS VARCHAR(MAX)) AS VaccineName,
		CAST(VaccineSpecialityID AS VARCHAR(MAX)) AS VaccineSpecialityID,
		CAST(VaccineTypeID AS VARCHAR(MAX)) AS VaccineTypeID,
		CAST(VaccineTypeName AS VARCHAR(MAX)) AS VaccineTypeName,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vVaccinations) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    