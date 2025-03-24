
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Förflyttningar av patient under inskrivning. Vid en första förflyttning läggs också ursprunglig vårdenhet till här. Notera att posterna lagras i omvänd kronologisk ordning.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EconomicalKombika': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TransferDatetime': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'CareUnitID': "{'title_ui': 'Flytta patient till', 'description': 'Den vårdenhet patienten flyttats till'}", 'EconomicalKombika': "{'title_ui': 'Ekonomisk enhet (ut)', 'description': 'Ny kombika/EXID'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt för registrering av förflyttningen'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som registrerat förflyttningen'}", 'TransferDatetime': "{'title_ui': 'Flyttad/Ändrad datum', 'description': 'När flytten skedde'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([EconomicalKombika] AS VARCHAR(MAX)) AS [EconomicalKombika],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [TransferDatetime], 126) AS [TransferDatetime] 
	FROM Intelligence.viewreader.vPAS_Inpatient_Transfers) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    