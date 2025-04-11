
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdministrationKey': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDatetime': 'varchar(max)', 'EventTypeID': 'varchar(max)', 'OrderCreatedAtCareUnitID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PrescriptionDate': 'varchar(max)', 'PrescriptionTime': 'varchar(max)', 'Rate': 'varchar(max)', 'RateUnit': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
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
		CONVERT(varchar(max), [AdministrationKey], 126) AS [AdministrationKey],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CONVERT(varchar(max), [EventDatetime], 126) AS [EventDatetime],
		CAST([EventTypeID] AS VARCHAR(MAX)) AS [EventTypeID],
		CAST([OrderCreatedAtCareUnitID] AS VARCHAR(MAX)) AS [OrderCreatedAtCareUnitID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PrescriptionDate], 126) AS [PrescriptionDate],
		CONVERT(varchar(max), [PrescriptionTime], 126) AS [PrescriptionTime],
		CAST([Rate] AS VARCHAR(MAX)) AS [Rate],
		CAST([RateUnit] AS VARCHAR(MAX)) AS [RateUnit],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved] 
	FROM Intelligence.viewreader.vMedOrders_OxygenTreatments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    