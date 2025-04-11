
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Register över de vårdenheter som finns inlagda i systemet. En vårdenhet är en arbetsplats av godtycklig storlek.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AddressRow1': 'varchar(max)', 'AddressRow2': 'varchar(max)', 'AddressRow3': 'varchar(max)', 'AllowedDelaySelfCheckinMinutes': 'varchar(max)', 'Bankgiro': 'varchar(max)', 'Beds': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'City': 'varchar(max)', 'CompanyID': 'varchar(max)', 'CountyID': 'varchar(max)', 'DefaultReferralNotificationMethodID': 'varchar(max)', 'DomainID': 'varchar(max)', 'DoseDispensationFirstDoseTime': 'varchar(max)', 'DoseDispensationPharmacyID': 'varchar(max)', 'DoseDispensationStopTime': 'varchar(max)', 'EAN': 'varchar(max)', 'Fax': 'varchar(max)', 'HSAID': 'varchar(max)', 'HasAutoUpdateListing': 'varchar(max)', 'HasCareUnitWeightValidity': 'varchar(max)', 'HasCoordinatedCarePlanning': 'varchar(max)', 'HasECoordinatedCarePlanning': 'varchar(max)', 'HasElectronicDoseDispensation': 'varchar(max)', 'HasManualOrdinationType': 'varchar(max)', 'HasRequiredFirstDoseTime': 'varchar(max)', 'HasRequiredInfusionTime': 'varchar(max)', 'HasRequiredOrdination': 'varchar(max)', 'HasSystemWeightValidity': 'varchar(max)', 'HasWebAppointments': 'varchar(max)', 'IsLabResultsRecipient': 'varchar(max)', 'IsOpenFiveDaysAWeek': 'varchar(max)', 'IsSendingSMSBookingNotificationForRooms': 'varchar(max)', 'IsUsingChildCare': 'varchar(max)', 'IsUsingDiagnosisSigningList': 'varchar(max)', 'IsUsingGroupBooking': 'varchar(max)', 'IsUsingSMSBookingNotification': 'varchar(max)', 'IsUsingVisitAndCaseReporting': 'varchar(max)', 'KatzMeasurementTemplateID': 'varchar(max)', 'KatzTermID': 'varchar(max)', 'Kombika': 'varchar(max)', 'LateBookingWarningDays': 'varchar(max)', 'Name': 'varchar(max)', 'PatientCancellationMessageID': 'varchar(max)', 'Plusgiro': 'varchar(max)', 'PostalCode': 'varchar(max)', 'RealCompanyCode': 'varchar(max)', 'TelephoneExternal': 'varchar(max)', 'TelephoneInternal': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'WeightDateOriginTypeID': 'varchar(max)', 'WeightTypeID': 'varchar(max)', 'WeightValidThroughDays': 'varchar(max)', 'WorkPlaceCode': 'varchar(max)'},
    column_descriptions={'CareUnitID': "{'title_ui': None, 'description': None}", 'Name': "{'title_ui': 'Vårdenhet', 'description': 'Vårdenhetens namn'}", 'AddressRow1': "{'title_ui': 'Adressrad 1', 'description': 'Kan vara det sjukhus vårdenheten tillhör'}", 'AddressRow2': "{'title_ui': 'Adressrad 2', 'description': 'Kan vara den klinik vårdenheten tillhör'}", 'AddressRow3': "{'title_ui': 'Adressrad 3', 'description': 'Kan vara den avdelning vårdenheten tillhör'}", 'DomainID': "{'title_ui': 'Vårdgivare', 'description': 'Den vårdgivare vårdenheten tillhör (hette tidigare domän)'}", 'PostalCode': "{'title_ui': 'Postadress', 'description': None}", 'City': "{'title_ui': 'Postadress', 'description': None}", 'CountyID': "{'title_ui': 'Län', 'description': 'Kod för det län som vårdenheten tillhör.'}", 'TelephoneInternal': "{'title_ui': 'Telefon (intern)', 'description': None}", 'TelephoneExternal': "{'title_ui': 'Telefon (extern)', 'description': None}", 'Fax': "{'title_ui': 'Telefax', 'description': None}", 'Kombika': "{'title_ui': 'Kod', 'description': 'Kombikakod/EXID som identifierar en enhet'}", 'EAN': "{'title_ui': 'EAN-kod', 'description': 'En globalt unik identifierare för vårdenheten'}", 'HSAID': "{'title_ui': 'HSA-id', 'description': 'Vårdenhetens HSAId. Nytt 2008-12.'}", 'Plusgiro': "{'title_ui': 'Plusgironummer', 'description': 'Vårdenhetens Plusgiro-konto (används exempelvis för e-recept)'}", 'Bankgiro': "{'title_ui': 'Bankgironummer', 'description': 'Vårdenhetens Bankgiro-konto. Nytt 2007-12.'}", 'CompanyID': '{\'title_ui\': \'Kund-kod\', \'description\': \'Den "kund" vårdenheten tillhör. Kallades tidigare företagskod. Här lagras ej företagskod, utan ett internt id som sedan kan slås upp mot företagskoden. Nytt 2007-12.\'}', 'RealCompanyCode': "{'title_ui': 'Företagskod', 'description': 'Det företag vårdenheten tillhör. Nytt 2009-03.'}", 'WorkPlaceCode': "{'title_ui': 'Arbetsplatskod', 'description': 'Används vid kommunikation med Apoteket'}", 'IsOpenFiveDaysAWeek': "{'title_ui': '5-dygnsvård', 'description': 'Om vårdenhetens arbetssätt är 5 dygn i veckan'}", 'Beds': "{'title_ui': 'Fastställda vårdplatser', 'description': 'Antal fastställda vårdplatser; inte nödvändigtvis antal disponibla vårdplatser'}", 'HasCoordinatedCarePlanning': "{'title_ui': 'Samordnad vårdplanering', 'description': 'Om vårdenheten gör samordnad vårdplanering'}", 'HasECoordinatedCarePlanning': "{'title_ui': 'E-samordnad vårdplanering', 'description': 'Om samordnad vårdplanering görs elektroniskt i TakeCare'}", 'IsLabResultsRecipient': "{'title_ui': 'Svarsmottagare labbsvar', 'description': 'Om vårdenheten är mottagare av elektroniska labbsvar'}", 'HasElectronicDoseDispensation': "{'title_ui': 'Dosdispensering', 'description': 'Om vårdenheten har elektronisk dosdispensering. Nytt 2007-12.'}", 'DoseDispensationStopTime': "{'title_ui': 'Stopptid dosdispensering', 'description': 'Stopp för dosdispensering. 00:00 är standardvärde. Nytt 2007-12.'}", 'DoseDispensationFirstDoseTime': "{'title_ui': 'Första dostid efter leverans', 'description': 'Första administreringstid som infaller efter leverans. 00:00 är standardvärde. Nytt 2007-12.'}", 'DoseDispensationPharmacyID': "{'title_ui': 'Dispenserande enhet', 'description': 'Det apotek som vårdenheten skickar sina rekvisitioner till. Nytt 2007-12.'}", 'IsUsingDiagnosisSigningList': "{'title_ui': 'Osignerad diagnos sparas inte i signeringslista', 'description': 'Om vårdenheten sparar diagnoser i signeringslista'}", 'KatzMeasurementTemplateID': "{'title_ui': 'Mätvärdesmall', 'description': None}", 'KatzTermID': "{'title_ui': 'Genererat värde', 'description': None}", 'IsUsingVisitAndCaseReporting': "{'title_ui': 'Ärende/Besöksrapportering', 'description': 'Vårdenheten har ärende- och besöksrapportering'}", 'HasRequiredInfusionTime': "{'title_ui': 'Infusionstid obligatorisk', 'description': 'Obligatoriskt på vårdenheten att ange infusionstid (varaktighet) när ordinationen är en infusion'}", 'HasRequiredFirstDoseTime': "{'title_ui': 'Klockslag för ord. gäller fr.o.m. fylls i vid spara', 'description': 'Obligatoriskt vid vårdenheten att fylla i klocklsag ordinationen gäller från'}", 'HasRequiredOrdination': "{'title_ui': 'Recept kan göras utan krav på ordination', 'description': 'Ordination måste finnas för recept på denna vårdenhet'}", 'HasManualOrdinationType': "{'title_ui': 'Manuellt val av ordinationstyp', 'description': 'Manuellt val av ordinationstyp är påslaget för vårdenheten. Automatiskt val är default'}", 'IsUsingSMSBookingNotification': "{'title_ui': 'SMS-påminnelse', 'description': 'Om vårdenheten har SMS-påminnelse för bokingar påslaget'}", 'IsSendingSMSBookingNotificationForRooms': "{'title_ui': 'Skicka SMS för rum och sängplatser', 'description': {'break': None}}", 'HasAutoUpdateListing': "{'title_ui': 'Uppdatera listningsuppgifter automatiskt.', 'description': None}", 'HasWebAppointments': '{\'title_ui\': \'Bokning via webben\', \'description\': \'Vårdenheten har bokning via "Mina vårdkontakter"\'}', 'AllowedDelaySelfCheckinMinutes': "{'title_ui': 'Tillåten försening vid ankomstregistrering', 'description': 'Antal minuter efter bokningens starttid som patienten själv ska kunna registrera sin ankomst.'}", 'WeightDateOriginTypeID': "{'title_ui': 'Datum (rimlighetskontroll)', 'description': {'break': [None, None, None, None]}}", 'WeightTypeID': "{'title_ui': 'Vikt i ord. fönstret', 'description': {'break': [None, None, None, None]}}", 'WeightValidThroughDays': "{'title_ui': 'Giltighet vikt, antal dagar', 'description': 'Giltighetstid i doseringshjälp/rimlighetskontroll för mätvärde vikt, i antal dagar. Kan sättas om fältet HasCareUnitWeightValidity är satt. Om giltighetstiden ej anges eller sätts till 0 så används 365 dagar.'}", 'HasCareUnitWeightValidity': "{'title_ui': 'Giltighet vikt, antal dagar', 'description': 'Anger om vårdenheten själv sätter giltighetstid i doseringshjälp/rimlighetskontroll (se fältet WeightValidThroughDays) för vikt'}", 'HasSystemWeightValidity': "{'title_ui': 'Giltighet vikt, generell tabell utifrån ålder', 'description': 'Om vårdenheten använder systemgemensam vikttabell i generella register för doseringshjälp/rimlighetskontroll.'}", 'IsUsingGroupBooking': "{'title_ui': 'Gruppbokning möjlig', 'description': 'Om vårdenheten har gruppbokning påslaget'}", 'IsUsingChildCare': "{'title_ui': 'BHV-funktionalitet', 'description': 'Om vårdenheten har BHV-funktionalitet påslagen'}", 'LateBookingWarningDays': "{'title_ui': 'Varning för väntetider i vården', 'description': {'break': None}}", 'ValidThroughDate': "{'title_ui': 'Stängd fr.o.m. datum', 'description': 'Det datum då vårdenheten eventuellt stängdes/inaktiverades. Om datum saknas är vårdenheten inte stängd utan aktiv.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['CareUnitID']
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
		CAST([AddressRow1] AS VARCHAR(MAX)) AS [AddressRow1],
		CAST([AddressRow2] AS VARCHAR(MAX)) AS [AddressRow2],
		CAST([AddressRow3] AS VARCHAR(MAX)) AS [AddressRow3],
		CAST([AllowedDelaySelfCheckinMinutes] AS VARCHAR(MAX)) AS [AllowedDelaySelfCheckinMinutes],
		CAST([Bankgiro] AS VARCHAR(MAX)) AS [Bankgiro],
		CAST([Beds] AS VARCHAR(MAX)) AS [Beds],
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([City] AS VARCHAR(MAX)) AS [City],
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([CountyID] AS VARCHAR(MAX)) AS [CountyID],
		CAST([DefaultReferralNotificationMethodID] AS VARCHAR(MAX)) AS [DefaultReferralNotificationMethodID],
		CAST([DomainID] AS VARCHAR(MAX)) AS [DomainID],
		CONVERT(varchar(max), [DoseDispensationFirstDoseTime], 126) AS [DoseDispensationFirstDoseTime],
		CAST([DoseDispensationPharmacyID] AS VARCHAR(MAX)) AS [DoseDispensationPharmacyID],
		CONVERT(varchar(max), [DoseDispensationStopTime], 126) AS [DoseDispensationStopTime],
		CAST([EAN] AS VARCHAR(MAX)) AS [EAN],
		CAST([Fax] AS VARCHAR(MAX)) AS [Fax],
		CAST([HSAID] AS VARCHAR(MAX)) AS [HSAID],
		CAST([HasAutoUpdateListing] AS VARCHAR(MAX)) AS [HasAutoUpdateListing],
		CAST([HasCareUnitWeightValidity] AS VARCHAR(MAX)) AS [HasCareUnitWeightValidity],
		CAST([HasCoordinatedCarePlanning] AS VARCHAR(MAX)) AS [HasCoordinatedCarePlanning],
		CAST([HasECoordinatedCarePlanning] AS VARCHAR(MAX)) AS [HasECoordinatedCarePlanning],
		CAST([HasElectronicDoseDispensation] AS VARCHAR(MAX)) AS [HasElectronicDoseDispensation],
		CAST([HasManualOrdinationType] AS VARCHAR(MAX)) AS [HasManualOrdinationType],
		CAST([HasRequiredFirstDoseTime] AS VARCHAR(MAX)) AS [HasRequiredFirstDoseTime],
		CAST([HasRequiredInfusionTime] AS VARCHAR(MAX)) AS [HasRequiredInfusionTime],
		CAST([HasRequiredOrdination] AS VARCHAR(MAX)) AS [HasRequiredOrdination],
		CAST([HasSystemWeightValidity] AS VARCHAR(MAX)) AS [HasSystemWeightValidity],
		CAST([HasWebAppointments] AS VARCHAR(MAX)) AS [HasWebAppointments],
		CAST([IsLabResultsRecipient] AS VARCHAR(MAX)) AS [IsLabResultsRecipient],
		CAST([IsOpenFiveDaysAWeek] AS VARCHAR(MAX)) AS [IsOpenFiveDaysAWeek],
		CAST([IsSendingSMSBookingNotificationForRooms] AS VARCHAR(MAX)) AS [IsSendingSMSBookingNotificationForRooms],
		CAST([IsUsingChildCare] AS VARCHAR(MAX)) AS [IsUsingChildCare],
		CAST([IsUsingDiagnosisSigningList] AS VARCHAR(MAX)) AS [IsUsingDiagnosisSigningList],
		CAST([IsUsingGroupBooking] AS VARCHAR(MAX)) AS [IsUsingGroupBooking],
		CAST([IsUsingSMSBookingNotification] AS VARCHAR(MAX)) AS [IsUsingSMSBookingNotification],
		CAST([IsUsingVisitAndCaseReporting] AS VARCHAR(MAX)) AS [IsUsingVisitAndCaseReporting],
		CAST([KatzMeasurementTemplateID] AS VARCHAR(MAX)) AS [KatzMeasurementTemplateID],
		CAST([KatzTermID] AS VARCHAR(MAX)) AS [KatzTermID],
		CAST([Kombika] AS VARCHAR(MAX)) AS [Kombika],
		CAST([LateBookingWarningDays] AS VARCHAR(MAX)) AS [LateBookingWarningDays],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([PatientCancellationMessageID] AS VARCHAR(MAX)) AS [PatientCancellationMessageID],
		CAST([Plusgiro] AS VARCHAR(MAX)) AS [Plusgiro],
		CAST([PostalCode] AS VARCHAR(MAX)) AS [PostalCode],
		CAST([RealCompanyCode] AS VARCHAR(MAX)) AS [RealCompanyCode],
		CAST([TelephoneExternal] AS VARCHAR(MAX)) AS [TelephoneExternal],
		CAST([TelephoneInternal] AS VARCHAR(MAX)) AS [TelephoneInternal],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate],
		CAST([WeightDateOriginTypeID] AS VARCHAR(MAX)) AS [WeightDateOriginTypeID],
		CAST([WeightTypeID] AS VARCHAR(MAX)) AS [WeightTypeID],
		CAST([WeightValidThroughDays] AS VARCHAR(MAX)) AS [WeightValidThroughDays],
		CAST([WorkPlaceCode] AS VARCHAR(MAX)) AS [WorkPlaceCode] 
	FROM Intelligence.viewreader.vCodes_CareUnits) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    