
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'RelationDocumentID': 'varchar(max)', 'RelationTypeCode': 'varchar(max)', 'RelationTypeDisplayName': 'varchar(max)', 'RelationWebcertCode': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={},
    kind=dict(
        name=ModelKindName.FULL
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
		CAST([RelationDocumentID] AS VARCHAR(MAX)) AS [RelationDocumentID],
		CAST([RelationTypeCode] AS VARCHAR(MAX)) AS [RelationTypeCode],
		CAST([RelationTypeDisplayName] AS VARCHAR(MAX)) AS [RelationTypeDisplayName],
		CAST([RelationWebcertCode] AS VARCHAR(MAX)) AS [RelationWebcertCode],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vWebcertCertificates_Relations) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    