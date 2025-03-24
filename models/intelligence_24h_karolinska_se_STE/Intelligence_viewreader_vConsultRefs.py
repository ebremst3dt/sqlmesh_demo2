
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Konsultationsbeställningar (remisser). Skickas från remittent (beställare) till bedömare/svarande (mottagare). Svaret skickas sedan till svarsmottagaren. Vid vidarebefordran skapas en ny version med ny svarande enhet. Beställningar kan både skickas iväg och tas emot, liksom ankomstregistreras (pappersremisser). Remisser kan skickas mellan olika system och då kan interna id:n som vårdenhet saknas.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AlphanumericalReferralID': 'varchar(max)', 'ArrivalDate': 'varchar(max)', 'ArrivalTime': 'varchar(max)', 'AssesserName': 'varchar(max)', 'AssesserUserID': 'varchar(max)', 'AssesserUserName': 'varchar(max)', 'CaseComment': 'varchar(max)', 'CaseHistory': 'varchar(max)', 'CaseStatusID': 'varchar(max)', 'CauseID': 'varchar(max)', 'ConfirmedByUserID': 'varchar(max)', 'ConfirmedDateTime': 'varchar(max)', 'ContentVersion': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DeclinedByName': 'varchar(max)', 'DeclinedByUserID': 'varchar(max)', 'DeclinedByUserName': 'varchar(max)', 'DeclinedComment': 'varchar(max)', 'DeclinedDate': 'varchar(max)', 'DeclinedReasonTermID': 'varchar(max)', 'DeclinedTime': 'varchar(max)', 'DesiredRequestRecipient': 'varchar(max)', 'DiagnosisProblem': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EndDate': 'varchar(max)', 'EndTime': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'InterpreterRequirement': 'varchar(max)', 'InvoiceeAddress': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeKombika': 'varchar(max)', 'IsBasisForCarePlan': 'varchar(max)', 'IsElectronic': 'varchar(max)', 'IsElectronicFromExternalSystem': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'IsPatientInitiated': 'varchar(max)', 'IsSpecialistReferral': 'varchar(max)', 'IsTracked': 'varchar(max)', 'IsWaiting': 'varchar(max)', 'NotificationTypeID': 'varchar(max)', 'PatientID': 'varchar(max)', 'RecipientCareUnitAddress': 'varchar(max)', 'RecipientCareUnitEAN': 'varchar(max)', 'RecipientCareUnitExternalID': 'varchar(max)', 'RecipientCareUnitID': 'varchar(max)', 'RecipientCareUnitKombika': 'varchar(max)', 'RecipientSID': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralID': 'varchar(max)', 'ReferralTime': 'varchar(max)', 'ReferringCareUnitAddress': 'varchar(max)', 'ReferringCareUnitEAN': 'varchar(max)', 'ReferringCareUnitExternalID': 'varchar(max)', 'ReferringCareUnitID': 'varchar(max)', 'ReferringCareUnitKombika': 'varchar(max)', 'ReferringCareUnitTelephone': 'varchar(max)', 'ReferringDoctorHSAID': 'varchar(max)', 'ReferringDoctorName': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringSID': 'varchar(max)', 'RegistrationStatus': 'varchar(max)', 'ReplyRecipientCareUnitAddress': 'varchar(max)', 'ReplyRecipientCareUnitEAN': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyRecipientSID': 'varchar(max)', 'RequestedExamination': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByHSAID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SpecReferralValidThroughDate': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TimestampSent': 'varchar(max)', 'TimestampSigned': 'varchar(max)', 'TrackingDate': 'varchar(max)', 'TrackingRemovalCauseTermID': 'varchar(max)', 'TrackingStatusID': 'varchar(max)', 'Version': 'varchar(max)', 'WarningDatetime': 'varchar(max)', 'WatchStatusID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatus': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': None}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': None}", 'SignedByHSAID': "{'title_ui': None, 'description': 'Hsaid på signerare, används i elektroniska remisser'}", 'TimestampSigned': "{'title_ui': 'Signerad', 'description': None}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'När remissen skapats. Antingen när den lästs in (för elektronisk remiss) eller när ankomstregistrering skett.'}", 'CaseComment': "{'title_ui': 'Ärendekommentar', 'description': 'Allmän kommentar (före 2006-01-28: kommentar för hänvisning)'}", 'CaseStatusID': "{'title_ui': 'Ärendestatus', 'description': 'Kallas även bedömningsstatus'}", 'WatchStatusID': "{'title_ui': 'Status', 'description': 'Bevakningsstatus för utgående externa remisser, sätts vid inläsning av remissmeddelande.'}", 'IsSpecialistReferral': "{'title_ui': 'Specialistvårdsremiss', 'description': 'Om remissen kommer från professionell vårdgivare i annat landsting'}", 'SpecReferralValidThroughDate': "{'title_ui': 'Specialistvårdsremiss giltig t.o.m.', 'description': 'Sista giltighetsdag för specialistvårdsremiss'}", 'ReferralDate': "{'title_ui': 'Remissdatum', 'description': 'När remissen skrevs (datum)'}", 'ReferralTime': "{'title_ui': 'Remisstid', 'description': 'När remissen skrevs (klockslag)'}", 'ArrivalDate': "{'title_ui': 'Ankomstdatum', 'description': 'När remissen ankom (datum). Anges manuellt för pappersremiss, sätts annars vid inläsning.'}", 'ArrivalTime': "{'title_ui': 'Ankomsttid', 'description': 'När remissen ankom (klockslag). Anges manuellt för pappersremiss, sätts annars vid inläsning.'}", 'EndDate': "{'title_ui': 'Avslutad datum', 'description': 'När remissen avslutades (datum)'}", 'EndTime': "{'title_ui': 'Avslutad tid', 'description': 'När remissen avslutades (klockslag)'}", 'TimestampSent': "{'title_ui': None, 'description': 'När beställningen är skickad av externt system. Sätts av inläsningsmaskin för elektroniska beställningar, och sätts alltså inte vid interna beställningar eller ankomstregistrering.'}", 'ReferringDoctorUserID': "{'title_ui': 'Signeringsansvarig/remittent', 'description': 'Även vidimeringsansvarig för det mottagna svaret'}", 'ReferringDoctorName': "{'title_ui': 'Signeringsansvarig/remittent', 'description': 'Remittentens namn om personnummer saknas, ex. vid pappersremiss'}", 'ReferringDoctorHSAID': "{'title_ui': None, 'description': 'Hsaid på remittent, används i elektroniska remisser'}", 'ReferringCareUnitID': "{'title_ui': 'Remitterande enhet', 'description': 'Tillika beställare. Alla på denna vårdenhet får se denna beställning. Kan vara NULL om ej elektronisk remiss.'}", 'ReferringCareUnitKombika': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets kombika/EXID'}", 'ReferringCareUnitAddress': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets adress'}", 'ReferringCareUnitTelephone': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets telefonnummer'}", 'ReferringCareUnitEAN': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets EAN-kod'}", 'ReferringSID': "{'title_ui': None, 'description': 'Remitterande system'}", 'AssesserUserID': "{'title_ui': 'Ansvarig bedömare', 'description': 'Den som är ansvarig för bedömningen (fr.o.m. sommaren 2007, tidigare lagrades endast användarnamn). Fylls i på mottagarsidan.'}", 'AssesserUserName': "{'title_ui': 'Ansvarig bedömare', 'description': None}", 'AssesserName': "{'title_ui': 'Ansvarig bedömare', 'description': 'Ansvarig bedömare om personnummer saknas, ex. vid pappersremiss'}", 'RecipientCareUnitID': "{'title_ui': 'Svarande enhet', 'description': 'Mottagare av remiss. Styr behörighet. Om remissen skickats utanför TakeCare lagras istället den avsändande enheten.'}", 'RecipientCareUnitKombika': "{'title_ui': 'Svarande enhet', 'description': 'Svarande enhets kombika/EXID'}", 'RecipientCareUnitAddress': "{'title_ui': 'Svarande enhet', 'description': 'Svarande enhets adress'}", 'RecipientCareUnitEAN': "{'title_ui': 'Svarande enhet', 'description': 'Svarande enhets EAN-kod'}", 'RecipientSID': "{'title_ui': None, 'description': 'Svarande system'}", 'InvoiceeKombika': "{'title_ui': None, 'description': 'Fakturamottagarens kombika/EXID. Samma som beställande enhet.'}", 'InvoiceeAddress': "{'title_ui': None, 'description': 'Fakturamottagarens adress'}", 'ReplyRecipientCareUnitID': "{'title_ui': None, 'description': 'Svarsmottagande enhet. Samma som beställande enhet.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': None, 'description': 'Svarsmottagande enhets kombika/EXID'}", 'ReplyRecipientCareUnitAddress': "{'title_ui': None, 'description': 'Svarsmottagande enhets adress'}", 'ReplyRecipientCareUnitEAN': "{'title_ui': None, 'description': 'Svarsmottagande enhets EAN-kod'}", 'ReplyRecipientSID': "{'title_ui': None, 'description': 'Svarsmottagande system'}", 'DeclinedByUserID': "{'title_ui': 'Hänvisad av', 'description': 'Den som är ansvarig för att ha hänvisat (fr.o.m. sommaren 2007, tidigare lagrades endast användarnamn)'}", 'DeclinedByUserName': "{'title_ui': 'Hänvisad av', 'description': None}", 'DeclinedByName': "{'title_ui': 'Hänvisad av', 'description': 'Den som har hänvisat remissen om personnummer saknas, ex. vid pappersremiss'}", 'DeclinedDate': "{'title_ui': 'Hänvisad datum', 'description': 'När hänvisning skett'}", 'DeclinedTime': "{'title_ui': 'Hänvisad tid', 'description': None}", 'DeclinedReasonTermID': "{'title_ui': 'Hänvisad orsak', 'description': None}", 'DeclinedComment': "{'title_ui': 'Hänvisad fri text', 'description': 'Kommentar till hänvisning'}", 'IsEmergency': "{'title_ui': 'Akut', 'description': None}", 'IsPatientInitiated': "{'title_ui': 'Patient sökt själv', 'description': 'Om patienten sökt vård själv (valfrihetspatient)'}", 'IsWaiting': "{'title_ui': 'Avvakta', 'description': 'Om bedömaren valt att avvakta. Infördes 2006-01-28'}", 'CaseHistory': "{'title_ui': 'Anamnes, status, upplysning', 'description': None}", 'CauseID': '{\'title_ui\': \'Konsultationsorsak\', \'description\': \'Fram till 2002 fanns också en orsak 3500 för "annan frågeställning".\'}', 'DiagnosisProblem': "{'title_ui': 'Diagnos/fråga', 'description': 'Frågeställning'}", 'RequestedExamination': "{'title_ui': 'Önskad undersökning', 'description': None}", 'IsTracked': "{'title_ui': None, 'description': 'Avser gamla bevakningen som användes fram till 2009, ska ej användas för statistik'}", 'TrackingStatusID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'TrackingDate': "{'title_ui': None, 'description': 'Avser gamla bevakningen som användes fram till 2009, ska ej användas för statistik - Datum då remissen skulle vara omhändertagen av mottagaren.'}", 'TrackingRemovalCauseTermID': "{'title_ui': None, 'description': 'Avser gamla bevakningen som användes fram till 2009, ska ej användas för statistik. Orsak till borttag från bevakningen.'}", 'IsElectronic': "{'title_ui': None, 'description': 'Om remissen är elektronisk'}", 'ReferralID': "{'title_ui': 'Remissidentitet', 'description': 'Kan saknas för ankomstregistrerade pappersremisser.'}", 'AlphanumericalReferralID': "{'title_ui': 'Remissidentitet', 'description': 'Alfanumerisk RID då detta förekommer'}", 'IsBasisForCarePlan': "{'title_ui': None, 'description': 'Om det finns en vårdplan/bokningsunderlag som skapats från denna remiss'}", 'ExternalUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för beställningen'}", 'ReferringCareUnitExternalID': "{'title_ui': 'Remitterande enhet', 'description': 'Kod för extern enhet för beställare'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': None, 'description': 'Kod för extern enhet för svarsmottagare'}", 'InvoiceeCareUnitExternalID': "{'title_ui': None, 'description': 'Kod för extern enhet för fakturamottagare'}", 'RecipientCareUnitExternalID': "{'title_ui': 'Svarande enhet', 'description': 'Kod för extern enhet för mottagare'}", 'NotificationTypeID': "{'title_ui': 'Bekräftelsesätt', 'description': 'Hur remissen har bekräftats, används fr.o.m. v. 21.1 endast för e-brev.'}", 'ConfirmedByUserID': "{'title_ui': 'Bekräftad', 'description': 'Remiss bekräftad av.'}", 'ConfirmedDateTime': "{'title_ui': 'Bekräftad', 'description': 'Tidpunkt då remiss bekräftades.'}", 'WarningDatetime': "{'title_ui': 'Varning', 'description': 'Tidpunkt då varningar hämtades till remissen. På externa elektroniska lagras remissdatum.'}", 'DesiredRequestRecipient': "{'title_ui': 'Önskad mottagare', 'description': 'Önskad mottagare av remissen'}", 'InterpreterRequirement': "{'title_ui': 'Tolkbehov', 'description': 'Patientens tolkbehov'}", 'IsElectronicFromExternalSystem': "{'title_ui': None, 'description': 'Om remissen skapats av inläsare för externa elektroniska remisser'}", 'ContentVersion': "{'title_ui': None, 'description': 'Versionsnummer på remissen. Anges av avsändaren. Versionen skall räknas upp vid en förändring av innehållet i remissen, exempelvis komplettering.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AlphanumericalReferralID] AS VARCHAR(MAX)) AS [AlphanumericalReferralID],
		CONVERT(varchar(max), [ArrivalDate], 126) AS [ArrivalDate],
		CONVERT(varchar(max), [ArrivalTime], 126) AS [ArrivalTime],
		CAST([AssesserName] AS VARCHAR(MAX)) AS [AssesserName],
		CAST([AssesserUserID] AS VARCHAR(MAX)) AS [AssesserUserID],
		CAST([AssesserUserName] AS VARCHAR(MAX)) AS [AssesserUserName],
		CAST([CaseComment] AS VARCHAR(MAX)) AS [CaseComment],
		CAST([CaseHistory] AS VARCHAR(MAX)) AS [CaseHistory],
		CAST([CaseStatusID] AS VARCHAR(MAX)) AS [CaseStatusID],
		CAST([CauseID] AS VARCHAR(MAX)) AS [CauseID],
		CAST([ConfirmedByUserID] AS VARCHAR(MAX)) AS [ConfirmedByUserID],
		CONVERT(varchar(max), [ConfirmedDateTime], 126) AS [ConfirmedDateTime],
		CAST([ContentVersion] AS VARCHAR(MAX)) AS [ContentVersion],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CAST([DeclinedByName] AS VARCHAR(MAX)) AS [DeclinedByName],
		CAST([DeclinedByUserID] AS VARCHAR(MAX)) AS [DeclinedByUserID],
		CAST([DeclinedByUserName] AS VARCHAR(MAX)) AS [DeclinedByUserName],
		CAST([DeclinedComment] AS VARCHAR(MAX)) AS [DeclinedComment],
		CONVERT(varchar(max), [DeclinedDate], 126) AS [DeclinedDate],
		CAST([DeclinedReasonTermID] AS VARCHAR(MAX)) AS [DeclinedReasonTermID],
		CONVERT(varchar(max), [DeclinedTime], 126) AS [DeclinedTime],
		CAST([DesiredRequestRecipient] AS VARCHAR(MAX)) AS [DesiredRequestRecipient],
		CAST([DiagnosisProblem] AS VARCHAR(MAX)) AS [DiagnosisProblem],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [EndDate], 126) AS [EndDate],
		CONVERT(varchar(max), [EndTime], 126) AS [EndTime],
		CAST([ExternalUnitIdTypeCode] AS VARCHAR(MAX)) AS [ExternalUnitIdTypeCode],
		CAST([InterpreterRequirement] AS VARCHAR(MAX)) AS [InterpreterRequirement],
		CAST([InvoiceeAddress] AS VARCHAR(MAX)) AS [InvoiceeAddress],
		CAST([InvoiceeCareUnitExternalID] AS VARCHAR(MAX)) AS [InvoiceeCareUnitExternalID],
		CAST([InvoiceeKombika] AS VARCHAR(MAX)) AS [InvoiceeKombika],
		CAST([IsBasisForCarePlan] AS VARCHAR(MAX)) AS [IsBasisForCarePlan],
		CAST([IsElectronic] AS VARCHAR(MAX)) AS [IsElectronic],
		CAST([IsElectronicFromExternalSystem] AS VARCHAR(MAX)) AS [IsElectronicFromExternalSystem],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([IsPatientInitiated] AS VARCHAR(MAX)) AS [IsPatientInitiated],
		CAST([IsSpecialistReferral] AS VARCHAR(MAX)) AS [IsSpecialistReferral],
		CAST([IsTracked] AS VARCHAR(MAX)) AS [IsTracked],
		CAST([IsWaiting] AS VARCHAR(MAX)) AS [IsWaiting],
		CAST([NotificationTypeID] AS VARCHAR(MAX)) AS [NotificationTypeID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([RecipientCareUnitAddress] AS VARCHAR(MAX)) AS [RecipientCareUnitAddress],
		CAST([RecipientCareUnitEAN] AS VARCHAR(MAX)) AS [RecipientCareUnitEAN],
		CAST([RecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [RecipientCareUnitExternalID],
		CAST([RecipientCareUnitID] AS VARCHAR(MAX)) AS [RecipientCareUnitID],
		CAST([RecipientCareUnitKombika] AS VARCHAR(MAX)) AS [RecipientCareUnitKombika],
		CAST([RecipientSID] AS VARCHAR(MAX)) AS [RecipientSID],
		CONVERT(varchar(max), [ReferralDate], 126) AS [ReferralDate],
		CAST([ReferralID] AS VARCHAR(MAX)) AS [ReferralID],
		CONVERT(varchar(max), [ReferralTime], 126) AS [ReferralTime],
		CAST([ReferringCareUnitAddress] AS VARCHAR(MAX)) AS [ReferringCareUnitAddress],
		CAST([ReferringCareUnitEAN] AS VARCHAR(MAX)) AS [ReferringCareUnitEAN],
		CAST([ReferringCareUnitExternalID] AS VARCHAR(MAX)) AS [ReferringCareUnitExternalID],
		CAST([ReferringCareUnitID] AS VARCHAR(MAX)) AS [ReferringCareUnitID],
		CAST([ReferringCareUnitKombika] AS VARCHAR(MAX)) AS [ReferringCareUnitKombika],
		CAST([ReferringCareUnitTelephone] AS VARCHAR(MAX)) AS [ReferringCareUnitTelephone],
		CAST([ReferringDoctorHSAID] AS VARCHAR(MAX)) AS [ReferringDoctorHSAID],
		CAST([ReferringDoctorName] AS VARCHAR(MAX)) AS [ReferringDoctorName],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringSID] AS VARCHAR(MAX)) AS [ReferringSID],
		CAST([RegistrationStatus] AS VARCHAR(MAX)) AS [RegistrationStatus],
		CAST([ReplyRecipientCareUnitAddress] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitAddress],
		CAST([ReplyRecipientCareUnitEAN] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitEAN],
		CAST([ReplyRecipientCareUnitExternalID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitExternalID],
		CAST([ReplyRecipientCareUnitID] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitID],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CAST([ReplyRecipientSID] AS VARCHAR(MAX)) AS [ReplyRecipientSID],
		CAST([RequestedExamination] AS VARCHAR(MAX)) AS [RequestedExamination],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SignedByHSAID] AS VARCHAR(MAX)) AS [SignedByHSAID],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SpecReferralValidThroughDate], 126) AS [SpecReferralValidThroughDate],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [TimestampSent], 126) AS [TimestampSent],
		CONVERT(varchar(max), [TimestampSigned], 126) AS [TimestampSigned],
		CONVERT(varchar(max), [TrackingDate], 126) AS [TrackingDate],
		CAST([TrackingRemovalCauseTermID] AS VARCHAR(MAX)) AS [TrackingRemovalCauseTermID],
		CAST([TrackingStatusID] AS VARCHAR(MAX)) AS [TrackingStatusID],
		CAST([Version] AS VARCHAR(MAX)) AS [Version],
		CONVERT(varchar(max), [WarningDatetime], 126) AS [WarningDatetime],
		CAST([WatchStatusID] AS VARCHAR(MAX)) AS [WatchStatusID] 
	FROM Intelligence.viewreader.vConsultRefs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    