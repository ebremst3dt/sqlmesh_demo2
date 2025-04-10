
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Produkter som angetts i KÖKS-formuläret (ytterligare egenskaper på produkterna i PAS_ProductsKOKS, dock ej produkter som -EJ och -IL)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Price': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Weight': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'ProductID': "{'title_ui': None, 'description': None}", 'ProductCode': "{'title_ui': None, 'description': 'Produkttyp och produktkod'}", 'Price': '{\'title_ui\': None, \'description\': \'Produktens viktade pris. Är vikten gånger en "konstant" för varje sjukhus, som revideras varje år. Priset här är vägledande och inte slutgiltigt.\'}', 'Weight': "{'title_ui': None, 'description': 'Produktvikt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID', 'ProductID']
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
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Price] AS VARCHAR(MAX)) AS [Price],
		CAST([ProductCode] AS VARCHAR(MAX)) AS [ProductCode],
		CAST([ProductID] AS VARCHAR(MAX)) AS [ProductID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Weight] AS VARCHAR(MAX)) AS [Weight] 
	FROM Intelligence.viewreader.vPAS_ProductsKOKS2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    