
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Utskrivningsklar (hette tidigare medicinskt färdigbehandlad). När patienten anses vara färdig för utskrivning. Påverkar ersättning från kommunen vid samordnad vårdplanering.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DischargeReadyDate': 'varchar(max)', 'DischargeReadyTime': 'varchar(max)', 'DischargeStatus': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ResponsibleUserID': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tid då data registrerats'}", 'DischargeStatus': "{'title_ui': 'Utskrivningsklar/Återtagen', 'description': {'break': None}}", 'DischargeReadyDate': "{'title_ui': 'Datum', 'description': 'Datum för utskrivningsklar/återtagen'}", 'DischargeReadyTime': "{'title_ui': 'Tid', 'description': 'Tid för utskrivningsklar/återtagen'}", 'ResponsibleUserID': "{'title_ui': 'Ansvarig läkare', 'description': 'Den användare som fattat beslutet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), [DischargeReadyDate], 126) AS [DischargeReadyDate],
		CONVERT(varchar(max), [DischargeReadyTime], 126) AS [DischargeReadyTime],
		CAST([DischargeStatus] AS VARCHAR(MAX)) AS [DischargeStatus],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ResponsibleUserID] AS VARCHAR(MAX)) AS [ResponsibleUserID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved] 
	FROM Intelligence.viewreader.vPAS_Inpatient_DischargeReady) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    