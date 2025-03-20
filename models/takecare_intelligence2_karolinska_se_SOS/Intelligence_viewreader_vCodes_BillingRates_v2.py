
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Taxor i kassan. En uppsättning koder per företag.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AccountID': 'varchar(max)', 'BillingRateCode': 'varchar(max)', 'BillingRateID': 'varchar(max)', 'CompanyID': 'varchar(max)', 'HasInvoiceInformation': 'varchar(max)', 'HasNoReductions': 'varchar(max)', 'HasOutOfCountyLogic': 'varchar(max)', 'IsHalfRate': 'varchar(max)', 'IsMedicareEligible': 'varchar(max)', 'IsMedicareInCountyDefault': 'varchar(max)', 'IsMedicareOutOfCountyDefault': 'varchar(max)', 'IsRateDoctorEditable': 'varchar(max)', 'IsRateOtherEditable': 'varchar(max)', 'IsRefund': 'varchar(max)', 'Name': 'varchar(max)', 'RateDoctor': 'varchar(max)', 'RateOther': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'BillingRateID': "{'title_ui': 'Id', 'description': None}", 'BillingRateCode': "{'title_ui': 'Taxekod', 'description': None}", 'CompanyID': "{'title_ui': 'Kundkoder', 'description': None}", 'Name': "{'title_ui': 'Taxenamn', 'description': None}", 'RateDoctor': "{'title_ui': 'Avgift läkare', 'description': None}", 'IsRateDoctorEditable': "{'title_ui': 'Användaren kan ange pris.', 'description': None}", 'RateOther': "{'title_ui': 'Avgift icke läkare', 'description': None}", 'IsRateOtherEditable': "{'title_ui': 'Användaren kan ange pris.', 'description': None}", 'HasOutOfCountyLogic': "{'title_ui': 'Taxan ger utomlänslogik, dvs storkundsfaktureras.', 'description': None}", 'IsHalfRate': "{'title_ui': 'Halvtaxa för <=19år', 'description': None}", 'IsMedicareEligible': "{'title_ui': 'Ej Frikortsgrundande.', 'description': None}", 'IsMedicareInCountyDefault': "{'title_ui': 'Inomlänare med frikort får denna taxa default.', 'description': None}", 'IsMedicareOutOfCountyDefault': "{'title_ui': 'Utomlänare med frikort får denna taxa default.', 'description': None}", 'HasInvoiceInformation': "{'title_ui': 'Skapa inte fakturaunderlag, dvs ingen storkundsfakturering.', 'description': None}", 'IsRefund': "{'title_ui': 'Återbetalning av intern/extern patientavgift.', 'description': None}", 'AccountID': "{'title_ui': 'Konto', 'description': 'ID för kontot taxan är kopplad till'}", 'HasNoReductions': "{'title_ui': 'Inga avgiftsreduceringar', 'description': 'Taxan kan inte ha några avgiftsreduceringar'}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': 'Första datum då data är giltigt'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AccountID AS VARCHAR(MAX)) AS AccountID,
		CAST(BillingRateCode AS VARCHAR(MAX)) AS BillingRateCode,
		CAST(BillingRateID AS VARCHAR(MAX)) AS BillingRateID,
		CAST(CompanyID AS VARCHAR(MAX)) AS CompanyID,
		CAST(HasInvoiceInformation AS VARCHAR(MAX)) AS HasInvoiceInformation,
		CAST(HasNoReductions AS VARCHAR(MAX)) AS HasNoReductions,
		CAST(HasOutOfCountyLogic AS VARCHAR(MAX)) AS HasOutOfCountyLogic,
		CAST(IsHalfRate AS VARCHAR(MAX)) AS IsHalfRate,
		CAST(IsMedicareEligible AS VARCHAR(MAX)) AS IsMedicareEligible,
		CAST(IsMedicareInCountyDefault AS VARCHAR(MAX)) AS IsMedicareInCountyDefault,
		CAST(IsMedicareOutOfCountyDefault AS VARCHAR(MAX)) AS IsMedicareOutOfCountyDefault,
		CAST(IsRateDoctorEditable AS VARCHAR(MAX)) AS IsRateDoctorEditable,
		CAST(IsRateOtherEditable AS VARCHAR(MAX)) AS IsRateOtherEditable,
		CAST(IsRefund AS VARCHAR(MAX)) AS IsRefund,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(RateDoctor AS VARCHAR(MAX)) AS RateDoctor,
		CAST(RateOther AS VARCHAR(MAX)) AS RateOther,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidFromDate, 126) AS ValidFromDate,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_BillingRates_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    