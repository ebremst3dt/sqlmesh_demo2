
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Paket som finns under beställningskategorierna snabbval och smittämnen och vilka provmaterial som visas under resp. paket. (Virologlabb)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Batch': 'varchar(max)', 'BatchID': 'varchar(max)', 'Category': 'varchar(max)', 'IsAllAnalysesRequired': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'BatchID': "{'title_ui': None, 'description': 'Paketid. Alltid unikt med ett undantag då det finns två provmaterial i samma paket.'}", 'Batch': "{'title_ui': None, 'description': 'Paketets namn'}", 'SpecimenID': "{'title_ui': None, 'description': 'I detta paket ska analyserna endast visas med dessa provmaterial. 0=ta provmaterial från Codes_VirSpecAnalyses istället'}", 'IsAllAnalysesRequired': "{'title_ui': None, 'description': 'Alla analyser inom paketet är obligatoriska'}", 'Category': "{'title_ui': None, 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['BatchID', 'SpecimenID']
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
		CAST([Batch] AS VARCHAR(MAX)) AS [Batch],
		CAST([BatchID] AS VARCHAR(MAX)) AS [BatchID],
		CAST([Category] AS VARCHAR(MAX)) AS [Category],
		CAST([IsAllAnalysesRequired] AS VARCHAR(MAX)) AS [IsAllAnalysesRequired],
		CAST([SpecimenID] AS VARCHAR(MAX)) AS [SpecimenID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_VirBatchesSpecimens) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    