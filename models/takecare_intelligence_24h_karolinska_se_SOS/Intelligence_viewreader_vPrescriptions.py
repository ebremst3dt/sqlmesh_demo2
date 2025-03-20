
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Recept",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdministrationMethodCode': 'varchar(max)', 'Amount': 'varchar(max)', 'BankGiroNumber': 'varchar(max)', 'BenefitTypeCode': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnit': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUser': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DatabaseID': 'varchar(max)', 'DeliveryInformationToPharmacy': 'varchar(max)', 'DiagnosisCode': 'varchar(max)', 'DiagnosisText': 'varchar(max)', 'DispensationIntervalTime': 'varchar(max)', 'DispensationIntervalTimeUnit': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Dosage': 'varchar(max)', 'DosageCode': 'varchar(max)', 'DosageForm': 'varchar(max)', 'DosageStrength': 'varchar(max)', 'ErrorAcknowledgement': 'varchar(max)', 'ErrorAcknowledgementCode': 'varchar(max)', 'HasTestPackage': 'varchar(max)', 'InstructionLanguageISO1Code': 'varchar(max)', 'InstructionLanguageText': 'varchar(max)', 'IsExchangeable': 'varchar(max)', 'IsRequestingFee': 'varchar(max)', 'MaxNumberOfDispensations': 'varchar(max)', 'MaxNumberOfDispensationsCode': 'varchar(max)', 'MedOrdersDocumentID': 'varchar(max)', 'MessageToPharmacy': 'varchar(max)', 'PatientBenefitTypeID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Pharmacy': 'varchar(max)', 'PharmacyID': 'varchar(max)', 'PostalGiroNumber': 'varchar(max)', 'PreparationName': 'varchar(max)', 'PrescriptionCancellationCause': 'varchar(max)', 'PrescriptionCancellationCauseCode': 'varchar(max)', 'PrescriptionCancellationUUID': 'varchar(max)', 'PrescriptionID': 'varchar(max)', 'PrescriptionTypeCode': 'varchar(max)', 'ProblemText': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedByRoleID': 'varchar(max)', 'SavedByUser': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TreatmentTime': 'varchar(max)', 'TreatmentTimeUnit': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'ValidThroughMonths': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None, None]}}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Värdet sattes tidigare bara vid makulering och var annars Null. Från 2009-04-03 sätts värde också när man sparar receptet första gången.'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedByRoleID': "{'title_ui': None, 'description': 'Roll-id på användaren som sparat dokumentet'}", 'SavedByUser': "{'title_ui': 'Version skapad av', 'description': 'Namn på den som skriver.'}", 'TimestampCreated': "{'title_ui': 'Händelsetid', 'description': None}", 'CreatedByUserID': "{'title_ui': 'Förskrivare', 'description': 'Original skapat av'}", 'CreatedByUser': "{'title_ui': 'Förskrivare', 'description': 'Namn på förskrivare. Ansvarig skapat recept.'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Vårdenhets ID, original (dokumentet skapat på denna arb.plats). Styr behörigheter. Ändras inte efter att dokumentet skapats. Kunde dock felaktigt ändras vid makulering. Detta rättades till 2009-10-09.'}", 'CreatedAtCareUnit': "{'title_ui': 'Tillhör vårdenhet', 'description': None}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Oftast preparatnamn. Kan föregås av <MAKULERAD>. Synlig i lista över recept.'}", 'DatabaseID': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'MedOrdersDocumentID': "{'title_ui': None, 'description': 'Komponentnummer som identifierar till vilken läkemedelsordination ett recept hör. Null om receptet skapats fristående.'}", 'PharmacyID': "{'title_ui': 'EAN-kod', 'description': 'Svenskt apoteks-id. EAN (GS1)-kod till Apoteket.'}", 'Pharmacy': "{'title_ui': 'Välj Apotek', 'description': 'Namn på valt apotek.'}", 'DeliveryInformationToPharmacy': "{'title_ui': 'Leveransinfo', 'description': 'Leveransinfo till apoteket'}", 'MessageToPharmacy': "{'title_ui': 'Meddelande', 'description': 'Meddelande till apoteket'}", 'InstructionLanguageISO1Code': "{'title_ui': None, 'description': 'Anvisning på språk, kod. Kod enligt ISO639-1.'}", 'InstructionLanguageText': "{'title_ui': 'Anvisning på', 'description': 'Språknamnet utskrivet på svenska.'}", 'PrescriptionID': "{'title_ui': 'e-receptid', 'description': 'Innehåller SLL e-receptid eller UUID (Universally unique identifier (Microsofts) om receptet skickats elektroniskt, annars Null.'}", 'ErrorAcknowledgementCode': "{'title_ui': 'Meddelande från e-recept', 'description': 'Kvittenskod vid fel. Ny 2008-06-27, Returkod från e-recept tjänst. Finns bara om kolumn RegistrationStatus är 6 eller 7, annars Null.'}", 'ErrorAcknowledgement': "{'title_ui': 'Meddelande från e-recept', 'description': 'Kvittenstext vid fel. Ny 2008-06-27, Returtext från e-recept tjänst. Finns bara om kolumn RegistrationStatus är 6 eller 7, annars Null.'}", 'IsRequestingFee': "{'title_ui': 'Jag begär arvode', 'description': 'Från 2003-12-02'}", 'Amount': "{'title_ui': 'Belopp (SEK)', 'description': 'Begärt arvodesbelopp'}", 'PostalGiroNumber': "{'title_ui': 'Plusgironr', 'description': 'För arvodesinbetalning, post'}", 'BankGiroNumber': "{'title_ui': 'Bankgironr', 'description': 'För arvodesinbetalning, bank'}", 'PrescriptionTypeCode': "{'title_ui': 'Recept/Recept för särskilda läkemendel/Hjälpmedelskort/Livsmedelsanvisning', 'description': {'break': [None, None, None, None, None]}}", 'PreparationName': "{'title_ui': 'Preparatnamn', 'description': None}", 'DosageForm': "{'title_ui': 'Läkemedelsform', 'description': 'Hette tidigare Beredningsform'}", 'DosageStrength': "{'title_ui': 'Styrka', 'description': None}", 'DosageCode': "{'title_ui': 'Dosering/Dygnsbehov', 'description': 'Dygnsbehov, kod eller egen (kort) text. Skrivs ut i klartext i första ledet i kolumn Dosage.'}", 'AdministrationMethodCode': "{'title_ui': 'Adm. sätt', 'description': 'Administrationssätt, kod eller egen (kort) text. Skrivs ut i klartext i andra ledet i kolumn Dosage.'}", 'ProblemText': "{'title_ui': 'Orsak', 'description': 'Problemtext (kort). Skrivs ut i tredje ledet i kolumn Dosage.'}", 'Dosage': "{'title_ui': 'Dosering, användning, ändamål', 'description': 'Dosering, lång textbeskrivning sammansatt med hjälp av kolumnerna DosageCode, AdministrationMethodCode och ProblemText. Används bara för vanliga recept och narkotika.'}", 'TreatmentTime': "{'title_ui': 'Behandlingstid', 'description': 'Behandlingstid, siffervärde.'}", 'TreatmentTimeUnit': '{\'title_ui\': \'Behandlingstid\', \'description\': \'Behandlingstid, tidsenhet, t.ex. "månader".\'}', 'MaxNumberOfDispensationsCode': "{'title_ui': 'Får expedieras', 'description': 'Antal gånger receptet får expedieras, siffra.'}", 'MaxNumberOfDispensations': "{'title_ui': 'Får expedieras', 'description': 'Antal gånger receptet får expedieras, text.'}", 'DispensationIntervalTime': "{'title_ui': 'Exp intervall', 'description': 'Tidsangivelse för exp intervall, siffra'}", 'DispensationIntervalTimeUnit': '{\'title_ui\': \'Exp intervall\', \'description\': \'Tidsangivelse för exp intervall, tidsenhet, t.ex. "månader".\'}', 'ValidThroughMonths': "{'title_ui': 'Giltighetstid', 'description': 'Giltighetstid - antal månader receptet är giltigt. Ersätts 2007-09-30 med faktiskt datum, se kolumn ValidThroughDate'}", 'ValidThroughDate': "{'title_ui': 'Giltighetstid', 'description': 'Ny 2007-09-30. Ersätter kolumn ValidThroughMonths. Anger när första uttag måste ske. Om uttag ej skett före detta datum anses receptet vara förbrukat.'}", 'BenefitTypeCode': "{'title_ui': 'Förmånstyp', 'description': {'break': [None, None, None, None, None, None, None]}}", 'PatientBenefitTypeID': "{'title_ui': 'Förmånsberättigad', 'description': {'break': [None, None, None]}}", 'DiagnosisCode': "{'title_ui': 'Diagnos', 'description': 'Diagnoskod. Hör ihop med kolumn DiagnosisText. Kan väljas från patientens diagnoser om diagnosmodulen är aktiverad. Kan skrivas in för hand.'}", 'DiagnosisText': "{'title_ui': 'Diagnos', 'description': 'Diagnostext. Hör ihop med kolumn DiagnosisCode. Kan väljas från patientens diagnoser om diagnosmodulen är aktiverad. Kan skrivas in för hand.'}", 'HasTestPackage': "{'title_ui': 'Med startförpackning', 'description': 'Med provförpackning/Startförpackning'}", 'IsExchangeable': "{'title_ui': 'Får ej bytas ut', 'description': 'Får bytas ut'}", 'PrescriptionCancellationUUID': "{'title_ui': 'Elektroniskt makulerings-id', 'description': 'UUID för den elektroniskt skickade makuleringen'}", 'PrescriptionCancellationCauseCode': "{'title_ui': 'Makuleringsorsak', 'description': 'Orsakskod för den elektroniskt skickade makuleringen'}", 'PrescriptionCancellationCause': "{'title_ui': 'Annan', 'description': 'Användarspecificerad makuleringsorsak'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AdministrationMethodCode AS VARCHAR(MAX)) AS AdministrationMethodCode,
		CAST(Amount AS VARCHAR(MAX)) AS Amount,
		CAST(BankGiroNumber AS VARCHAR(MAX)) AS BankGiroNumber,
		CAST(BenefitTypeCode AS VARCHAR(MAX)) AS BenefitTypeCode,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnit AS VARCHAR(MAX)) AS CreatedAtCareUnit,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUser AS VARCHAR(MAX)) AS CreatedByUser,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DatabaseID AS VARCHAR(MAX)) AS DatabaseID,
		CAST(DeliveryInformationToPharmacy AS VARCHAR(MAX)) AS DeliveryInformationToPharmacy,
		CAST(DiagnosisCode AS VARCHAR(MAX)) AS DiagnosisCode,
		CAST(DiagnosisText AS VARCHAR(MAX)) AS DiagnosisText,
		CAST(DispensationIntervalTime AS VARCHAR(MAX)) AS DispensationIntervalTime,
		CAST(DispensationIntervalTimeUnit AS VARCHAR(MAX)) AS DispensationIntervalTimeUnit,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Dosage AS VARCHAR(MAX)) AS Dosage,
		CAST(DosageCode AS VARCHAR(MAX)) AS DosageCode,
		CAST(DosageForm AS VARCHAR(MAX)) AS DosageForm,
		CAST(DosageStrength AS VARCHAR(MAX)) AS DosageStrength,
		CAST(ErrorAcknowledgement AS VARCHAR(MAX)) AS ErrorAcknowledgement,
		CAST(ErrorAcknowledgementCode AS VARCHAR(MAX)) AS ErrorAcknowledgementCode,
		CAST(HasTestPackage AS VARCHAR(MAX)) AS HasTestPackage,
		CAST(InstructionLanguageISO1Code AS VARCHAR(MAX)) AS InstructionLanguageISO1Code,
		CAST(InstructionLanguageText AS VARCHAR(MAX)) AS InstructionLanguageText,
		CAST(IsExchangeable AS VARCHAR(MAX)) AS IsExchangeable,
		CAST(IsRequestingFee AS VARCHAR(MAX)) AS IsRequestingFee,
		CAST(MaxNumberOfDispensations AS VARCHAR(MAX)) AS MaxNumberOfDispensations,
		CAST(MaxNumberOfDispensationsCode AS VARCHAR(MAX)) AS MaxNumberOfDispensationsCode,
		CAST(MedOrdersDocumentID AS VARCHAR(MAX)) AS MedOrdersDocumentID,
		CAST(MessageToPharmacy AS VARCHAR(MAX)) AS MessageToPharmacy,
		CAST(PatientBenefitTypeID AS VARCHAR(MAX)) AS PatientBenefitTypeID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Pharmacy AS VARCHAR(MAX)) AS Pharmacy,
		CAST(PharmacyID AS VARCHAR(MAX)) AS PharmacyID,
		CAST(PostalGiroNumber AS VARCHAR(MAX)) AS PostalGiroNumber,
		CAST(PreparationName AS VARCHAR(MAX)) AS PreparationName,
		CAST(PrescriptionCancellationCause AS VARCHAR(MAX)) AS PrescriptionCancellationCause,
		CAST(PrescriptionCancellationCauseCode AS VARCHAR(MAX)) AS PrescriptionCancellationCauseCode,
		CAST(PrescriptionCancellationUUID AS VARCHAR(MAX)) AS PrescriptionCancellationUUID,
		CAST(PrescriptionID AS VARCHAR(MAX)) AS PrescriptionID,
		CAST(PrescriptionTypeCode AS VARCHAR(MAX)) AS PrescriptionTypeCode,
		CAST(ProblemText AS VARCHAR(MAX)) AS ProblemText,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(SavedByRoleID AS VARCHAR(MAX)) AS SavedByRoleID,
		CAST(SavedByUser AS VARCHAR(MAX)) AS SavedByUser,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(TreatmentTime AS VARCHAR(MAX)) AS TreatmentTime,
		CAST(TreatmentTimeUnit AS VARCHAR(MAX)) AS TreatmentTimeUnit,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate,
		CAST(ValidThroughMonths AS VARCHAR(MAX)) AS ValidThroughMonths,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPrescriptions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    