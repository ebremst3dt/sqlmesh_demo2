
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassabesök. Vid makulering skapas en ny transaktion (makuleringstransaktion), och den ursprungliga flaggas som makulerad. Det kan också skapas transaktioner som endast är fakturaunderlag.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AppointmentAtCareUnitID': 'varchar(max)', 'AppointmentID': 'varchar(max)', 'AppointmentTime': 'varchar(max)', 'AppointmentWithResourceID': 'varchar(max)', 'AppointmentWithUserName': 'varchar(max)', 'BillingFeeReductionID': 'varchar(max)', 'BillingOfficeOpenedByUserName': 'varchar(max)', 'BillingOfficeOpenedDatetime': 'varchar(max)', 'CancellationDate': 'varchar(max)', 'CashierUserName': 'varchar(max)', 'CompanyID': 'varchar(max)', 'ContractAmount': 'varchar(max)', 'ContractCustomerID': 'varchar(max)', 'CustomerGroupCode': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EmergencyID': 'varchar(max)', 'FreecardError': 'varchar(max)', 'InvoiceComment': 'varchar(max)', 'InvoiceEmergencyID': 'varchar(max)', 'InvoiceFee': 'varchar(max)', 'InvoiceNumber': 'varchar(max)', 'InvoiceRateID': 'varchar(max)', 'InvoiceReferringKombika': 'varchar(max)', 'InvoiceVisitTypeID': 'varchar(max)', 'InvoicerUserName': 'varchar(max)', 'IsCancelled': 'varchar(max)', 'IsInvoiceInformation': 'varchar(max)', 'IsInvoiceable': 'varchar(max)', 'IsInvoiced': 'varchar(max)', 'IsOnCredit': 'varchar(max)', 'IsPayedByOther': 'varchar(max)', 'IsRefund': 'varchar(max)', 'IsRetail': 'varchar(max)', 'IsULPOrContractCustomer': 'varchar(max)', 'IsUpToFreecard': 'varchar(max)', 'ListingReadDate': 'varchar(max)', 'MedicalCenter': 'varchar(max)', 'MedicareCardNumber': 'varchar(max)', 'MedicareCardProviderID': 'varchar(max)', 'MedicareCardProviderTransactionUUID': 'varchar(max)', 'MedicareCardTransactionUUID': 'varchar(max)', 'MedicareCardUUID': 'varchar(max)', 'MedicareCardValidThroughDate': 'varchar(max)', 'MixedPaymentCashPart': 'varchar(max)', 'NursingHomeAdmissionDate': 'varchar(max)', 'NursingHomeName': 'varchar(max)', 'OtherPayerID': 'varchar(max)', 'PatientFee': 'varchar(max)', 'PatientID': 'varchar(max)', 'PaymentMethodID': 'varchar(max)', 'RateID': 'varchar(max)', 'RateOriginal': 'varchar(max)', 'ReceiptNumber': 'varchar(max)', 'ReceiptNumberOriginal': 'varchar(max)', 'RefundDatetime': 'varchar(max)', 'RefundDocumentID': 'varchar(max)', 'RefundReasonID': 'varchar(max)', 'RefundedDocumentID': 'varchar(max)', 'SIVLMANumber': 'varchar(max)', 'SIVLMAValidThroughDate': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionDate': 'varchar(max)', 'VisitLocationCode': 'varchar(max)', 'VisitTypeCategoryID': 'varchar(max)', 'VisitTypeID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'CompanyID': "{'title_ui': None, 'description': 'Det företag där registrering skett. Företagskoden styr vilka kodtabeller som används vid kassaregistrering.'}", 'VisitTypeID': "{'title_ui': 'Besökstyp', 'description': 'Typ av besök'}", 'EmergencyID': "{'title_ui': 'Akut', 'description': 'Om besöket var akut'}", 'BillingOfficeOpenedDatetime': "{'title_ui': 'Öppnad', 'description': 'Tidpunkt då kassan öppnades'}", 'BillingOfficeOpenedByUserName': "{'title_ui': 'Öppnad', 'description': 'Användare som öppnade kassan (användarnamn)'}", 'CashierUserName': "{'title_ui': None, 'description': 'Den som registrerat besöket i kassan (användarnamn)'}", 'ReceiptNumber': "{'title_ui': 'Kvittonr', 'description': None}", 'ReceiptNumberOriginal': "{'title_ui': None, 'description': 'Om makuleringstransaktion'}", 'InvoiceVisitTypeID': "{'title_ui': 'Besökstyp', 'description': 'Besökstyp som anges vid fakturering'}", 'InvoiceEmergencyID': "{'title_ui': 'Akut', 'description': 'Akutkod som anges vid fakturering'}", 'InvoiceRateID': "{'title_ui': 'Taxa', 'description': 'Taxa som anges vid fakturering. Väljs antingen bland de vanliga taxorna eller bland särskilda taxor för storkunder.'}", 'InvoiceReferringKombika': "{'title_ui': 'Remitterande vårdenhet', 'description': 'Remitterande kombika/EXID som anges vid fakturering'}", 'InvoiceComment': "{'title_ui': 'Kompletterande fakturatext', 'description': 'Kommentar på faktura'}", 'IsPayedByOther': "{'title_ui': None, 'description': 'Om annan betalare'}", 'IsCancelled': "{'title_ui': None, 'description': 'Om transaktionen har makulerats. Markerar även hela PAS-posten som makulerad.'}", 'CancellationDate': "{'title_ui': None, 'description': 'Makuleringsdatum'}", 'IsULPOrContractCustomer': "{'title_ui': None, 'description': 'Om besöket gäller en utomlänspatient eller storkundspatient'}", 'IsOnCredit': "{'title_ui': None, 'description': 'Om kreditbesök'}", 'IsInvoiceable': "{'title_ui': None, 'description': 'Om ej borttagen från faktureringslista. (Är sann även om fakturering inte ska ske.)'}", 'IsInvoiceInformation': "{'title_ui': None, 'description': 'Om transaktionen endast är ett fakturaunderlag'}", 'IsInvoiced': "{'title_ui': None, 'description': 'Om transaktionen har skapat ett fakturaunderlag till ekonomi-systemet'}", 'IsRetail': "{'title_ui': None, 'description': 'Om transaktionen är hälsovård'}", 'IsRefund': "{'title_ui': None, 'description': 'Om transaktionen är återbetalning'}", 'RefundedDocumentID': "{'title_ui': None, 'description': 'Den transaktion som har blivit återbetald'}", 'RefundDocumentID': "{'title_ui': None, 'description': 'Transaktionen som är en återbetalning'}", 'RefundDatetime': "{'title_ui': 'Återbetalning', 'description': 'Tid återbetalningen skedde'}", 'RefundReasonID': "{'title_ui': 'Orsak', 'description': 'Orsaken till återbetalningen'}", 'MedicareCardUUID': "{'title_ui': None, 'description': 'UUID för patientens frikort. Ny 2015. Hämtas från extern frikortstjänst.'}", 'MedicareCardProviderID': "{'title_ui': None, 'description': 'Identifierar leverantören av frikortet. 1=CGI. Ny 2015.'}", 'MedicareCardTransactionUUID': "{'title_ui': None, 'description': 'UUID för frikortstransaktion. Ny 2015. Skickas med till extern frikortstjänst. Om detta värde är satt, har tjänsten anropats.'}", 'MedicareCardProviderTransactionUUID': "{'title_ui': None, 'description': 'UUID för frikortstransaktion. Ny 2015. Hämtas från extern frikortstjänst. Om detta värde är satt, har tjänsten svarat.'}", 'FreecardError': "{'title_ui': 'Felmeddelande från frikortstjänsten', 'description': None}", 'OtherPayerID': "{'title_ui': 'Betalare', 'description': 'Id för annan betalare vid kreditbesök'}", 'InvoiceNumber': "{'title_ui': None, 'description': 'Fakturanummer vid kreditbesök'}", 'RateID': "{'title_ui': 'Taxa', 'description': None}", 'PaymentMethodID': "{'title_ui': 'Betalningssätt', 'description': None}", 'PatientFee': "{'title_ui': 'Avgift', 'description': 'Den avgift patienten betalade'}", 'MixedPaymentCashPart': "{'title_ui': 'Varav kontanter', 'description': 'Hur mycket av betalningen som skedde i kontanter vid betalning med både kort och kontanter'}", 'RateOriginal': "{'title_ui': 'Originaltaxa', 'description': 'Hur mycket originaltaxa är för besöket'}", 'BillingFeeReductionID': "{'title_ui': 'Avgiftsreducering', 'description': 'ID för avgiftsreduceringsregel'}", 'IsUpToFreecard': "{'title_ui': 'Upp till frikort', 'description': 'Betalningen är upp till frikort'}", 'ContractCustomerID': "{'title_ui': 'Avtalskund', 'description': 'Id för avtalskund/storkund vid fakturering'}", 'ContractAmount': "{'title_ui': 'Avtalsbelopp', 'description': 'Det belopp avtalskunden ska betala. Anges vid fakturering.'}", 'MedicareCardNumber': "{'title_ui': 'Frikortsnummer', 'description': 'Nummer på patientens frikort'}", 'MedicareCardValidThroughDate': "{'title_ui': 'Frikort t.o.m.', 'description': 'Sista giltighetsdag frikort'}", 'SIVLMANumber': "{'title_ui': 'LMA-kortsnummer', 'description': 'Id-nummer från tillfälligt LMA-kort/SIV-kort (för asylsökande)'}", 'SIVLMAValidThroughDate': "{'title_ui': 'LMA-kort t.o.m.', 'description': 'Sista giltighetsdatum för LMA-kort/SIV-kort'}", 'AppointmentID': "{'title_ui': None, 'description': 'Id för bokningsunderlag. Den bokning som kassabesöket utgår från.'}", 'AppointmentAtCareUnitID': "{'title_ui': None, 'description': 'Vårdenhet där bokning skett'}", 'AppointmentWithResourceID': "{'title_ui': None, 'description': 'Bokad resurs för detta besök, exempelvis en användare eller yrkeskategori'}", 'AppointmentWithUserName': "{'title_ui': 'Bokad hos', 'description': 'Resurs som patient bokats hos (användarnamn). Ansvarig under besöket. Kan matas in manuellt, även om det finns en bokning kopplad till en annan resurs.'}", 'AppointmentTime': "{'title_ui': 'Bokad tid', 'description': 'Ev. tidpunkt för inbokat besök'}", 'InvoiceFee': "{'title_ui': None, 'description': 'Avgift som patient ska betala vid fakturering'}", 'InvoicerUserName': "{'title_ui': None, 'description': 'Den användare som utfört faktureringen'}", 'TransactionDate': "{'title_ui': 'Reg.datum', 'description': 'Datum då kassatransaktionen genomförts'}", 'CustomerGroupCode': "{'title_ui': None, 'description': 'Kundgrupp för vårdenheten, där kassabesöket skett (PAS.SavedAtCareUnitID)'}", 'ListingReadDate': "{'title_ui': 'Listningsuppgifter uppdaterade', 'description': 'Datum då informationen hämtats från listningssystem'}", 'NursingHomeName': "{'title_ui': 'Särskilt boende, namn', 'description': 'Namn på särskilt boende'}", 'NursingHomeAdmissionDate': "{'title_ui': 'Särskilt boende, inskrivningsdatum', 'description': 'Inskrivningsdatum på särskilt boende'}", 'MedicalCenter': "{'title_ui': 'Vårdcentralnamn', 'description': None}", 'VisitTypeCategoryID': "{'title_ui': 'Besöksform', 'description': None}", 'VisitLocationCode': "{'title_ui': 'Besöksplats', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AppointmentAtCareUnitID AS VARCHAR(MAX)) AS AppointmentAtCareUnitID,
		CAST(AppointmentID AS VARCHAR(MAX)) AS AppointmentID,
		CONVERT(varchar(max), AppointmentTime, 126) AS AppointmentTime,
		CAST(AppointmentWithResourceID AS VARCHAR(MAX)) AS AppointmentWithResourceID,
		CAST(AppointmentWithUserName AS VARCHAR(MAX)) AS AppointmentWithUserName,
		CAST(BillingFeeReductionID AS VARCHAR(MAX)) AS BillingFeeReductionID,
		CAST(BillingOfficeOpenedByUserName AS VARCHAR(MAX)) AS BillingOfficeOpenedByUserName,
		CONVERT(varchar(max), BillingOfficeOpenedDatetime, 126) AS BillingOfficeOpenedDatetime,
		CONVERT(varchar(max), CancellationDate, 126) AS CancellationDate,
		CAST(CashierUserName AS VARCHAR(MAX)) AS CashierUserName,
		CAST(CompanyID AS VARCHAR(MAX)) AS CompanyID,
		CAST(ContractAmount AS VARCHAR(MAX)) AS ContractAmount,
		CAST(ContractCustomerID AS VARCHAR(MAX)) AS ContractCustomerID,
		CAST(CustomerGroupCode AS VARCHAR(MAX)) AS CustomerGroupCode,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(EmergencyID AS VARCHAR(MAX)) AS EmergencyID,
		CAST(FreecardError AS VARCHAR(MAX)) AS FreecardError,
		CAST(InvoiceComment AS VARCHAR(MAX)) AS InvoiceComment,
		CAST(InvoiceEmergencyID AS VARCHAR(MAX)) AS InvoiceEmergencyID,
		CAST(InvoiceFee AS VARCHAR(MAX)) AS InvoiceFee,
		CAST(InvoiceNumber AS VARCHAR(MAX)) AS InvoiceNumber,
		CAST(InvoiceRateID AS VARCHAR(MAX)) AS InvoiceRateID,
		CAST(InvoiceReferringKombika AS VARCHAR(MAX)) AS InvoiceReferringKombika,
		CAST(InvoiceVisitTypeID AS VARCHAR(MAX)) AS InvoiceVisitTypeID,
		CAST(InvoicerUserName AS VARCHAR(MAX)) AS InvoicerUserName,
		CAST(IsCancelled AS VARCHAR(MAX)) AS IsCancelled,
		CAST(IsInvoiceInformation AS VARCHAR(MAX)) AS IsInvoiceInformation,
		CAST(IsInvoiceable AS VARCHAR(MAX)) AS IsInvoiceable,
		CAST(IsInvoiced AS VARCHAR(MAX)) AS IsInvoiced,
		CAST(IsOnCredit AS VARCHAR(MAX)) AS IsOnCredit,
		CAST(IsPayedByOther AS VARCHAR(MAX)) AS IsPayedByOther,
		CAST(IsRefund AS VARCHAR(MAX)) AS IsRefund,
		CAST(IsRetail AS VARCHAR(MAX)) AS IsRetail,
		CAST(IsULPOrContractCustomer AS VARCHAR(MAX)) AS IsULPOrContractCustomer,
		CAST(IsUpToFreecard AS VARCHAR(MAX)) AS IsUpToFreecard,
		CONVERT(varchar(max), ListingReadDate, 126) AS ListingReadDate,
		CAST(MedicalCenter AS VARCHAR(MAX)) AS MedicalCenter,
		CAST(MedicareCardNumber AS VARCHAR(MAX)) AS MedicareCardNumber,
		CAST(MedicareCardProviderID AS VARCHAR(MAX)) AS MedicareCardProviderID,
		CAST(MedicareCardProviderTransactionUUID AS VARCHAR(MAX)) AS MedicareCardProviderTransactionUUID,
		CAST(MedicareCardTransactionUUID AS VARCHAR(MAX)) AS MedicareCardTransactionUUID,
		CAST(MedicareCardUUID AS VARCHAR(MAX)) AS MedicareCardUUID,
		CONVERT(varchar(max), MedicareCardValidThroughDate, 126) AS MedicareCardValidThroughDate,
		CAST(MixedPaymentCashPart AS VARCHAR(MAX)) AS MixedPaymentCashPart,
		CONVERT(varchar(max), NursingHomeAdmissionDate, 126) AS NursingHomeAdmissionDate,
		CAST(NursingHomeName AS VARCHAR(MAX)) AS NursingHomeName,
		CAST(OtherPayerID AS VARCHAR(MAX)) AS OtherPayerID,
		CAST(PatientFee AS VARCHAR(MAX)) AS PatientFee,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PaymentMethodID AS VARCHAR(MAX)) AS PaymentMethodID,
		CAST(RateID AS VARCHAR(MAX)) AS RateID,
		CAST(RateOriginal AS VARCHAR(MAX)) AS RateOriginal,
		CAST(ReceiptNumber AS VARCHAR(MAX)) AS ReceiptNumber,
		CAST(ReceiptNumberOriginal AS VARCHAR(MAX)) AS ReceiptNumberOriginal,
		CONVERT(varchar(max), RefundDatetime, 126) AS RefundDatetime,
		CAST(RefundDocumentID AS VARCHAR(MAX)) AS RefundDocumentID,
		CAST(RefundReasonID AS VARCHAR(MAX)) AS RefundReasonID,
		CAST(RefundedDocumentID AS VARCHAR(MAX)) AS RefundedDocumentID,
		CAST(SIVLMANumber AS VARCHAR(MAX)) AS SIVLMANumber,
		CONVERT(varchar(max), SIVLMAValidThroughDate, 126) AS SIVLMAValidThroughDate,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TransactionDate, 126) AS TransactionDate,
		CAST(VisitLocationCode AS VARCHAR(MAX)) AS VisitLocationCode,
		CAST(VisitTypeCategoryID AS VARCHAR(MAX)) AS VisitTypeCategoryID,
		CAST(VisitTypeID AS VARCHAR(MAX)) AS VisitTypeID 
	FROM Intelligence.viewreader.vPAS_Billing) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    