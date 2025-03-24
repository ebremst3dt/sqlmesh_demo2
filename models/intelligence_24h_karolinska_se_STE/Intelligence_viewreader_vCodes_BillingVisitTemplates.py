
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kassa - Besöksmallar",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BillingRateCode': 'varchar(max)', 'BillingVisitTemplateID': 'varchar(max)', 'CareProviderID1': 'varchar(max)', 'CareProviderID2': 'varchar(max)', 'EmergencyWard': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'VisitLocationID': 'varchar(max)', 'VisitTemplateCode': 'varchar(max)', 'VisitTypeCategoryID': 'varchar(max)', 'VisitTypeCode': 'varchar(max)'},
    column_descriptions={'BillingVisitTemplateID': "{'title_ui': 'ID', 'description': 'Internt id för besöksmallen'}", 'VisitTemplateCode': "{'title_ui': 'Kod', 'description': None}", 'Name': "{'title_ui': 'Namn', 'description': None}", 'VisitTypeCode': "{'title_ui': 'Besökstyp', 'description': 'Besökstypskod'}", 'VisitTypeCategoryID': "{'title_ui': 'Besöksform', 'description': None}", 'VisitLocationID': "{'title_ui': 'Besöksplats', 'description': None}", 'EmergencyWard': "{'title_ui': 'Akut', 'description': '1=Akut, 2=Ej Akut'}", 'CareProviderID1': "{'title_ui': 'Yrkeskategori 1', 'description': 'Yrkeskategorin som utför hälsovård'}", 'CareProviderID2': "{'title_ui': 'Yrkeskategori 2', 'description': 'Yrkeskategorin som utför hälsovård'}", 'BillingRateCode': "{'title_ui': 'Taxa', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BillingRateCode] AS VARCHAR(MAX)) AS [BillingRateCode],
		CAST([BillingVisitTemplateID] AS VARCHAR(MAX)) AS [BillingVisitTemplateID],
		CAST([CareProviderID1] AS VARCHAR(MAX)) AS [CareProviderID1],
		CAST([CareProviderID2] AS VARCHAR(MAX)) AS [CareProviderID2],
		CAST([EmergencyWard] AS VARCHAR(MAX)) AS [EmergencyWard],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate],
		CAST([VisitLocationID] AS VARCHAR(MAX)) AS [VisitLocationID],
		CAST([VisitTemplateCode] AS VARCHAR(MAX)) AS [VisitTemplateCode],
		CAST([VisitTypeCategoryID] AS VARCHAR(MAX)) AS [VisitTypeCategoryID],
		CAST([VisitTypeCode] AS VARCHAR(MAX)) AS [VisitTypeCode] 
	FROM Intelligence.viewreader.vCodes_BillingVisitTemplates) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    