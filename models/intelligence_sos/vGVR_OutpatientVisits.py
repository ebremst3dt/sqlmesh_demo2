
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="GVR besöksregistreringar och korrigeringar av besöksregistreringar för öppenvården",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BillingRateCode': 'varchar(max)', 'CarePeriodCode': 'varchar(max)', 'CareProviderCode': 'varchar(max)', 'ClinicalPathwayNumber': 'varchar(max)', 'ContactTypeCode': 'varchar(max)', 'FileName': 'varchar(max)', 'HealthCareContract': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)', 'VisitTypeCode': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'VisitTypeCode': "{'title_ui': None, 'description': 'Besökstyp'}", 'CareProviderCode': "{'title_ui': None, 'description': 'Vårdgivare. Om flera vårdgivare väljs anges koderna i sekvens utan skiljetecken.'}", 'IsEmergency': "{'title_ui': None, 'description': 'Akutbesök'}", 'BillingRateCode': "{'title_ui': None, 'description': 'Avgiftsklassificering/Taxa'}", 'CarePeriodCode': "{'title_ui': None, 'description': 'Vårdperiod. Kopplar ihop öppenvårdsbesök med en vårdperiod.'}", 'HealthCareContract': "{'title_ui': None, 'description': 'Vårdavtal. Används ej.'}", 'ContactTypeCode': "{'title_ui': None, 'description': 'Kontakttyp. Gäller endast kontaktrapportering, ej kassaregistrering.'}", 'ClinicalPathwayNumber': "{'title_ui': None, 'description': 'Vårdkedjenummer. Används ej.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['FileName', 'TransactionID']
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
		CAST([BillingRateCode] AS VARCHAR(MAX)) AS [BillingRateCode],
		CAST([CarePeriodCode] AS VARCHAR(MAX)) AS [CarePeriodCode],
		CAST([CareProviderCode] AS VARCHAR(MAX)) AS [CareProviderCode],
		CAST([ClinicalPathwayNumber] AS VARCHAR(MAX)) AS [ClinicalPathwayNumber],
		CAST([ContactTypeCode] AS VARCHAR(MAX)) AS [ContactTypeCode],
		CAST([FileName] AS VARCHAR(MAX)) AS [FileName],
		CAST([HealthCareContract] AS VARCHAR(MAX)) AS [HealthCareContract],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransactionID] AS VARCHAR(MAX)) AS [TransactionID],
		CAST([VisitTypeCode] AS VARCHAR(MAX)) AS [VisitTypeCode] 
	FROM Intelligence.viewreader.vGVR_OutpatientVisits) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    