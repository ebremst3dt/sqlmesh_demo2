
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Uppgifter om ej debiterbara ekonomitransaktioner. Produktkoderna EJ, EJD och IL.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Amount': 'varchar(max)', 'CarePlanDocumentID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsAutopsied': 'varchar(max)', 'KOKSCode': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductType': 'varchar(max)', 'ProductTypeDRGGrouper': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'ProductTypeDRGGrouper': "{'title_ui': None, 'description': 'Typ av DRG-grupperare, ej produkttyp'}", 'ProductType': "{'title_ui': None, 'description': 'Den riktiga produkttypen'}", 'ProductCode': "{'title_ui': None, 'description': None}", 'Amount': "{'title_ui': None, 'description': 'Antal av denna produkt (alltid 1)'}", 'KOKSCode': "{'title_ui': 'Kod N/C-koder', 'description': 'Där N = 1-4'}", 'IsAutopsied': "{'title_ui': 'Obducerad', 'description': 'Om patienten har obducerats'}", 'CarePlanDocumentID': "{'title_ui': 'Kopplad vårdplanering', 'description': 'Dokument-id för kopplad vårdplan (från DRG-formuläret)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID']
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
		CAST([Amount] AS VARCHAR(MAX)) AS [Amount],
		CAST([CarePlanDocumentID] AS VARCHAR(MAX)) AS [CarePlanDocumentID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([IsAutopsied] AS VARCHAR(MAX)) AS [IsAutopsied],
		CAST([KOKSCode] AS VARCHAR(MAX)) AS [KOKSCode],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ProductCode] AS VARCHAR(MAX)) AS [ProductCode],
		CAST([ProductType] AS VARCHAR(MAX)) AS [ProductType],
		CAST([ProductTypeDRGGrouper] AS VARCHAR(MAX)) AS [ProductTypeDRGGrouper],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vPAS_NonDRG) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    