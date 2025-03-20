
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Patientuppgifter. Uppgifterna kommer främst från Personuppgiftssystemet (PU). -- vissa fält kan ändras av vårdpersonal i TakeCare. Uppdateringar sker inte regelbundet, så data från PU kan vara inaktuellt. Namn- och adressuppgifter kommer bara med om delsystem 'Intelligence - Personuppgifter i PatInfo-tabellen' är aktiverat i journalsystemet. Data från PU kan bara innehålla A-Z, 0-9, Å, Ä och Ö, samt viss interpunktion. Andra tecken översätts till sina 'försvenskade' motsvarigheter; Ü blir exempelvis U. (Vissa specialtecken kan enligt uppgift slinka igenom PU, och ingen vet vilken teckenkodning som används.) Tabellen innehåller inte vilken vårdenhet data är sparat på.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CFUBaseAreaCode': 'varchar(max)', 'CFUCounty': 'varchar(max)', 'CFUDateOfBirth': 'varchar(max)', 'CFUMaritalStatus': 'varchar(max)', 'CFUMaritalStatusDate': 'varchar(max)', 'CFUMunicipality': 'varchar(max)', 'CFUOrdererUnitCode': 'varchar(max)', 'CFUOrdererUnitName': 'varchar(max)', 'CFUParish': 'varchar(max)', 'CFUPsychiatryAreaCode': 'varchar(max)', 'CFUPsychiatryAreaName': 'varchar(max)', 'CFURetrievedTime': 'varchar(max)', 'DateOfBirth': 'varchar(max)', 'LMACardNumber': 'varchar(max)', 'LMACardValidThrough': 'varchar(max)', 'MedicareCardNumber': 'varchar(max)', 'MedicareCardProviderID': 'varchar(max)', 'MedicareCardUUID': 'varchar(max)', 'MedicareCardValidFrom': 'varchar(max)', 'MedicareCardValidThrough': 'varchar(max)', 'PatientID': 'varchar(max)', 'RecordCreatedTimestamp': 'varchar(max)', 'Sex': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DateOfBirth': "{'title_ui': 'Födelsedatum', 'description': 'Patientens födelsedatum'}", 'Sex': "{'title_ui': 'Kön', 'description': {'break': [None, None, None]}}", 'MedicareCardNumber': "{'title_ui': 'Frikortsnummer', 'description': 'Inmatat av användare: kan därför innehålla skräptecken'}", 'MedicareCardUUID': "{'title_ui': None, 'description': 'UUID för patientens frikort. Ny 2015. Hämtas från extern frikortstjänst.'}", 'MedicareCardProviderID': "{'title_ui': None, 'description': 'Identifierar leverantören av frikortet. 1=CGI Ny 2015.'}", 'MedicareCardValidFrom': "{'title_ui': 'Frikort fr.o.m.', 'description': 'Första giltighetsdag för frikort'}", 'MedicareCardValidThrough': "{'title_ui': 'Frikort t.o.m.', 'description': 'Sista giltighetsdag för frikort'}", 'LMACardNumber': "{'title_ui': 'LMA-kortsnummer', 'description': 'Inmatat av användare: kan därför innehålla skräptecken'}", 'LMACardValidThrough': "{'title_ui': 'LMA-kort t.o.m.', 'description': 'Sista giltighetsdag för LMA-kort'}", 'CFURetrievedTime': "{'title_ui': None, 'description': 'När data från PU (CFU) hämtats'}", 'CFUDateOfBirth': "{'title_ui': 'Födelsedatum', 'description': 'Patientens födelsedatum. Fram till 20111018 inte alltid giltigt datum'}", 'CFUMaritalStatus': "{'title_ui': 'Civilstånd', 'description': 'Patientens civilstånd enligt Personuppgiftssystemet'}", 'CFUMaritalStatusDate': "{'title_ui': None, 'description': 'Datum då patient inträtt i civilstånd enligt Personuppgiftssystemet. Inte alltid giltigt datum.'}", 'CFUCounty': "{'title_ui': 'Länskod', 'description': 'Kod för det län patienten tillhör.'}", 'CFUMunicipality': "{'title_ui': 'Kommunkod', 'description': 'Kod för den kommun patienten tillhör.'}", 'CFUParish': "{'title_ui': 'Församlingskod', 'description': 'Kod för den församling patienten tillhör. Efter 2015-12-31 i stort sett alltid 99 i TakeCare, på grund av regeländring.'}", 'CFUOrdererUnitCode': "{'title_ui': None, 'description': 'Beställaravdelning'}", 'CFUOrdererUnitName': "{'title_ui': 'Beställaravdelning', 'description': 'Kallades tidigare Sjukvårdsområde (SO)'}", 'CFUPsychiatryAreaCode': "{'title_ui': None, 'description': 'Områdeskod för psyksektor'}", 'CFUPsychiatryAreaName': "{'title_ui': None, 'description': 'Psyksektornamn'}", 'CFUBaseAreaCode': "{'title_ui': 'Basområdeskod', 'description': None}", 'RecordCreatedTimestamp': "{'title_ui': None, 'description': 'Tidsstämpel då journalen skapades i TakeCare'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(CFUBaseAreaCode AS VARCHAR(MAX)) AS CFUBaseAreaCode,
		CAST(CFUCounty AS VARCHAR(MAX)) AS CFUCounty,
		CAST(CFUDateOfBirth AS VARCHAR(MAX)) AS CFUDateOfBirth,
		CAST(CFUMaritalStatus AS VARCHAR(MAX)) AS CFUMaritalStatus,
		CAST(CFUMaritalStatusDate AS VARCHAR(MAX)) AS CFUMaritalStatusDate,
		CAST(CFUMunicipality AS VARCHAR(MAX)) AS CFUMunicipality,
		CAST(CFUOrdererUnitCode AS VARCHAR(MAX)) AS CFUOrdererUnitCode,
		CAST(CFUOrdererUnitName AS VARCHAR(MAX)) AS CFUOrdererUnitName,
		CAST(CFUParish AS VARCHAR(MAX)) AS CFUParish,
		CAST(CFUPsychiatryAreaCode AS VARCHAR(MAX)) AS CFUPsychiatryAreaCode,
		CAST(CFUPsychiatryAreaName AS VARCHAR(MAX)) AS CFUPsychiatryAreaName,
		CONVERT(varchar(max), CFURetrievedTime, 126) AS CFURetrievedTime,
		CONVERT(varchar(max), DateOfBirth, 126) AS DateOfBirth,
		CAST(LMACardNumber AS VARCHAR(MAX)) AS LMACardNumber,
		CONVERT(varchar(max), LMACardValidThrough, 126) AS LMACardValidThrough,
		CAST(MedicareCardNumber AS VARCHAR(MAX)) AS MedicareCardNumber,
		CAST(MedicareCardProviderID AS VARCHAR(MAX)) AS MedicareCardProviderID,
		CAST(MedicareCardUUID AS VARCHAR(MAX)) AS MedicareCardUUID,
		CONVERT(varchar(max), MedicareCardValidFrom, 126) AS MedicareCardValidFrom,
		CONVERT(varchar(max), MedicareCardValidThrough, 126) AS MedicareCardValidThrough,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), RecordCreatedTimestamp, 126) AS RecordCreatedTimestamp,
		CAST(Sex AS VARCHAR(MAX)) AS Sex,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vPatInfo) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    