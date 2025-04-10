
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Köpta artiklar under ett besök, kan också vara hälsovård.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ArticleCode': 'varchar(max)', 'ArticleID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsPreventiveHealthCare': 'varchar(max)', 'PatientID': 'varchar(max)', 'Price': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'Quantity': 'varchar(max)', 'Row': 'varchar(max)', 'SalesTax': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ArticleID': "{'title_ui': 'Artikel id', 'description': 'Index till artikeltabell'}", 'ArticleCode': "{'title_ui': 'Artikelkod', 'description': None}", 'Price': "{'title_ui': 'Artikelkod', 'description': None}", 'Quantity': "{'title_ui': 'Antal', 'description': None}", 'SalesTax': "{'title_ui': 'Moms', 'description': 'Momssatsen som togs ut vid köp'}", 'IsPreventiveHealthCare': "{'title_ui': 'Hälsovård', 'description': None}", 'ProfessionID': "{'title_ui': 'Yrkeskategori', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([ArticleCode] AS VARCHAR(MAX)) AS [ArticleCode],
		CAST([ArticleID] AS VARCHAR(MAX)) AS [ArticleID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([IsPreventiveHealthCare] AS VARCHAR(MAX)) AS [IsPreventiveHealthCare],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Price] AS VARCHAR(MAX)) AS [Price],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([Quantity] AS VARCHAR(MAX)) AS [Quantity],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SalesTax] AS VARCHAR(MAX)) AS [SalesTax],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vPAS_Billing_RetailArticles) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    