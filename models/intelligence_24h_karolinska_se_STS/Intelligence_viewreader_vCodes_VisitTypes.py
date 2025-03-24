
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Besökstyper för bokningar och kassa. En uppsättning koder per företag.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CompanyCode': 'varchar(max)', 'CompanyID': 'varchar(max)', 'CountryID': 'varchar(max)', 'CountyID': 'varchar(max)', 'FirstVisitPossible': 'varchar(max)', 'IsAcknowledged': 'varchar(max)', 'IsAutomaticallyApproved': 'varchar(max)', 'IsReferralTariffToBeUsed': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'VisitTypeCategoryID': 'varchar(max)', 'VisitTypeCode': 'varchar(max)', 'VisitTypeID': 'varchar(max)'},
    column_descriptions={'VisitTypeID': "{'title_ui': 'Id', 'description': 'Internt id för besökstypen'}", 'VisitTypeCode': "{'title_ui': 'Besökstyp', 'description': 'Besökstypskod'}", 'Name': "{'title_ui': 'Namn', 'description': 'Besökstyp i klartext'}", 'CompanyID': "{'title_ui': 'Kundkod', 'description': 'Det företag (internt ID) besökstypen gäller'}", 'CompanyCode': "{'title_ui': 'Kundkod', 'description': 'Det företag (kundkod) besökstypen gäller'}", 'CountyID': "{'title_ui': 'Län', 'description': 'Länskod'}", 'CountryID': "{'title_ui': 'Land', 'description': 'Landskod'}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': 'Första datum då besökstypen är giltig'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då besökstypen är giltig'}", 'IsAcknowledged': "{'title_ui': 'Kvitteras', 'description': 'Om en kvittens ska skickas till bokningen när patienten ankommit'}", 'VisitTypeCategoryID': "{'title_ui': 'Kategori', 'description': {'break': [None, None]}}", 'IsAutomaticallyApproved': "{'title_ui': 'AutomatGodkännas', 'description': None}", 'FirstVisitPossible': "{'title_ui': '1:a besök möjligt', 'description': {'break': [None, None, None]}}", 'IsReferralTariffToBeUsed': "{'title_ui': 'Remisstaxa', 'description': 'Anger om kassaposten ska ha remisstaxa'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CompanyCode] AS VARCHAR(MAX)) AS [CompanyCode],
		CAST([CompanyID] AS VARCHAR(MAX)) AS [CompanyID],
		CAST([CountryID] AS VARCHAR(MAX)) AS [CountryID],
		CAST([CountyID] AS VARCHAR(MAX)) AS [CountyID],
		CAST([FirstVisitPossible] AS VARCHAR(MAX)) AS [FirstVisitPossible],
		CAST([IsAcknowledged] AS VARCHAR(MAX)) AS [IsAcknowledged],
		CAST([IsAutomaticallyApproved] AS VARCHAR(MAX)) AS [IsAutomaticallyApproved],
		CAST([IsReferralTariffToBeUsed] AS VARCHAR(MAX)) AS [IsReferralTariffToBeUsed],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate],
		CAST([VisitTypeCategoryID] AS VARCHAR(MAX)) AS [VisitTypeCategoryID],
		CAST([VisitTypeCode] AS VARCHAR(MAX)) AS [VisitTypeCode],
		CAST([VisitTypeID] AS VARCHAR(MAX)) AS [VisitTypeID] 
	FROM Intelligence.viewreader.vCodes_VisitTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    