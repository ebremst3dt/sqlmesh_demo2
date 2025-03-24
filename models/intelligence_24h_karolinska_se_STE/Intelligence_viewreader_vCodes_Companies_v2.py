
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Företag (vårdproducenter) för öppenvården. Listan underhålls av kunden själv och stämmer inte nödvändigtvis med landstingets kodserver.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BICCode': 'varchar(max)', 'BankGiro': 'varchar(max)', 'CollectionFee': 'varchar(max)', 'CompanyCode': 'varchar(max)', 'CompanyGroup': 'varchar(max)', 'CompanyID': 'varchar(max)', 'County': 'varchar(max)', 'CountyID': 'varchar(max)', 'CreditDays': 'varchar(max)', 'EconomicalCatalogue': 'varchar(max)', 'HasExtendedInvoiceNo': 'varchar(max)', 'HasFTax': 'varchar(max)', 'IBAN': 'varchar(max)', 'InvoiceNoSeriesEnd': 'varchar(max)', 'InvoiceNoSeriesStart': 'varchar(max)', 'InvoicingCharge': 'varchar(max)', 'Logotype': 'varchar(max)', 'Name': 'varchar(max)', 'OrgNo': 'varchar(max)', 'PGOrBG': 'varchar(max)', 'PhoneNo': 'varchar(max)', 'PlusGiro': 'varchar(max)', 'PlusGiroContract': 'varchar(max)', 'PlusGiroRecipient': 'varchar(max)', 'PostalAddress': 'varchar(max)', 'RESFolder': 'varchar(max)', 'RESUser': 'varchar(max)', 'RealCompanyCode': 'varchar(max)', 'ReminderCharge': 'varchar(max)', 'SnodUser': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'VATRegistrationNumber': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'CompanyID': "{'title_ui': 'Id', 'description': None}", 'CompanyCode': "{'title_ui': 'Kundkod', 'description': 'Det som tidigare kallades företagskod, men egentligen var en kundkod'}", 'RealCompanyCode': "{'title_ui': 'FöretagsKod', 'description': 'Den riktiga företagskoden. Ny 2009-03'}", 'Name': "{'title_ui': 'Företagsnamn', 'description': None}", 'OrgNo': "{'title_ui': 'Organisationsnummer', 'description': None}", 'PostalAddress': "{'title_ui': 'Postnummer + ort', 'description': None}", 'PhoneNo': "{'title_ui': 'Telefonnummer', 'description': None}", 'CountyID': "{'title_ui': 'Länskod', 'description': 'Kod för det län som företaget tillhör -- eller specialkoder för andra indelningar'}", 'County': "{'title_ui': 'Län namn', 'description': 'Beteckning på länet. Inte nödvändigtvis länets namn.'}", 'BankGiro': "{'title_ui': 'Bankgiro', 'description': None}", 'PlusGiro': "{'title_ui': 'Plusgironummer', 'description': None}", 'PlusGiroContract': "{'title_ui': 'Postgiroavtal', 'description': None}", 'PlusGiroRecipient': "{'title_ui': 'PG-avi, betalningsmottagare', 'description': None}", 'PGOrBG': "{'title_ui': 'PG/BG', 'description': 'PlusGiro (PG) eller BankGiro (BG) för kreditfakturor'}", 'EconomicalCatalogue': "{'title_ui': 'Katalog för rapportering', 'description': 'Katalog för ekonomisk rapportering'}", 'InvoicingCharge': "{'title_ui': 'Expeditionsavgift öppenvård', 'description': None}", 'ReminderCharge': "{'title_ui': 'Påminnelseavgift, kreditfaktura', 'description': None}", 'CreditDays': "{'title_ui': 'Betalningsvillkor antal dagar', 'description': None}", 'InvoiceNoSeriesStart': "{'title_ui': 'Fakturanummerstart', 'description': 'Fakturanummerserie, start'}", 'InvoiceNoSeriesEnd': "{'title_ui': 'Fakturanummerslut', 'description': 'Fakturanummerserie, slut'}", 'RESUser': "{'title_ui': 'Användar ID mot RES', 'description': None}", 'RESFolder': "{'title_ui': 'Katalog för svarsfil från RES', 'description': None}", 'SnodUser': "{'title_ui': 'Snod-användarid', 'description': 'Snod-användarid överföring RES'}", 'IBAN': "{'title_ui': 'IBAN-nr', 'description': None}", 'BICCode': "{'title_ui': 'BIC-kod', 'description': None}", 'Logotype': "{'title_ui': 'Logo', 'description': None}", 'CompanyGroup': "{'title_ui': 'Kundgrupp', 'description': None}", 'HasFTax': "{'title_ui': 'Innehar F-skattebevis', 'description': 'Satt om företaget innehar ett F-skattebevis'}", 'VATRegistrationNumber': "{'title_ui': 'Momsregistreringsnummer', 'description': None}", 'CollectionFee': "{'title_ui': 'Inkassoavgift', 'description': None}", 'HasExtendedInvoiceNo': "{'title_ui': '12 siffror referensnummer i StgA', 'description': 'Anger om STGA-rapporten är utökad för 12 siffror referensnummer'}", 'ValidThroughDate': "{'title_ui': 'Giltig tom', 'description': 'Sista datum då data är giltigt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BICCode] AS VARCHAR(MAX)) AS [BICCode],
		CAST([BankGiro] AS VARCHAR(MAX)) AS [BankGiro],
		CAST([CollectionFee] AS VARCHAR(MAX)) AS [CollectionFee],
		CAST([CompanyCode] AS VARCHAR(MAX)) AS [CompanyCode],
		CAST([CompanyGroup] AS VARCHAR(MAX)) AS [CompanyGroup],
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([County] AS VARCHAR(MAX)) AS [County],
		CAST([CountyID] AS VARCHAR(MAX)) AS [CountyID],
		CAST([CreditDays] AS VARCHAR(MAX)) AS [CreditDays],
		CAST([EconomicalCatalogue] AS VARCHAR(MAX)) AS [EconomicalCatalogue],
		CAST([HasExtendedInvoiceNo] AS VARCHAR(MAX)) AS [HasExtendedInvoiceNo],
		CAST([HasFTax] AS VARCHAR(MAX)) AS [HasFTax],
		CAST([IBAN] AS VARCHAR(MAX)) AS [IBAN],
		CAST([InvoiceNoSeriesEnd] AS VARCHAR(MAX)) AS [InvoiceNoSeriesEnd],
		CAST([InvoiceNoSeriesStart] AS VARCHAR(MAX)) AS [InvoiceNoSeriesStart],
		CAST([InvoicingCharge] AS VARCHAR(MAX)) AS [InvoicingCharge],
		CAST([Logotype] AS VARCHAR(MAX)) AS [Logotype],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([OrgNo] AS VARCHAR(MAX)) AS [OrgNo],
		CAST([PGOrBG] AS VARCHAR(MAX)) AS [PGOrBG],
		CAST([PhoneNo] AS VARCHAR(MAX)) AS [PhoneNo],
		CAST([PlusGiro] AS VARCHAR(MAX)) AS [PlusGiro],
		CAST([PlusGiroContract] AS VARCHAR(MAX)) AS [PlusGiroContract],
		CAST([PlusGiroRecipient] AS VARCHAR(MAX)) AS [PlusGiroRecipient],
		CAST([PostalAddress] AS VARCHAR(MAX)) AS [PostalAddress],
		CAST([RESFolder] AS VARCHAR(MAX)) AS [RESFolder],
		CAST([RESUser] AS VARCHAR(MAX)) AS [RESUser],
		CAST([RealCompanyCode] AS VARCHAR(MAX)) AS [RealCompanyCode],
		CAST([ReminderCharge] AS VARCHAR(MAX)) AS [ReminderCharge],
		CAST([SnodUser] AS VARCHAR(MAX)) AS [SnodUser],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([VATRegistrationNumber] AS VARCHAR(MAX)) AS [VATRegistrationNumber],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_Companies_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    