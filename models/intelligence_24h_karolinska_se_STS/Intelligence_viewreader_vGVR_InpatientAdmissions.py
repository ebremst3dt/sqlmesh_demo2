
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="GVR inskrivningstransaktioner och korrigeringar av inskrivning, för slutenvården.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdmissionCode': 'varchar(max)', 'AdmissionFormCode': 'varchar(max)', 'CareConnection': 'varchar(max)', 'ClinicalPathwayNumber': 'varchar(max)', 'FileName': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'ReferringCareUnit': 'varchar(max)', 'ReferringClinic': 'varchar(max)', 'ReferringHospital': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)', 'TreatmentPeriod': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'ReferringHospital': "{'title_ui': None, 'description': 'Remitterande inrättning. Första delen av kombika.'}", 'ReferringClinic': "{'title_ui': None, 'description': 'Remitterande klinik. Andra delen av kombika.'}", 'ReferringCareUnit': "{'title_ui': None, 'description': 'Remitterande avdelning. Tredje delen av kombika.'}", 'AdmissionCode': "{'title_ui': None, 'description': 'Inskrivningskod. Anger varifrån patienten kom vid inskrivningen.'}", 'AdmissionFormCode': "{'title_ui': None, 'description': 'Psykiatrisk vårdform. Registreras enbart inom psykiatrin.'}", 'IsEmergency': "{'title_ui': None, 'description': 'Akutinskrivning'}", 'TreatmentPeriod': "{'title_ui': None, 'description': 'Psykvårdperiod'}", 'CareConnection': "{'title_ui': None, 'description': 'Vårdsamband. Används ej.'}", 'ClinicalPathwayNumber': "{'title_ui': None, 'description': 'Vårdkedjenummer. Används ej'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AdmissionCode] AS VARCHAR(MAX)) AS [AdmissionCode],
		CAST([AdmissionFormCode] AS VARCHAR(MAX)) AS [AdmissionFormCode],
		CAST([CareConnection] AS VARCHAR(MAX)) AS [CareConnection],
		CAST([ClinicalPathwayNumber] AS VARCHAR(MAX)) AS [ClinicalPathwayNumber],
		CAST([FileName] AS VARCHAR(MAX)) AS [FileName],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([ReferringCareUnit] AS VARCHAR(MAX)) AS [ReferringCareUnit],
		CAST([ReferringClinic] AS VARCHAR(MAX)) AS [ReferringClinic],
		CAST([ReferringHospital] AS VARCHAR(MAX)) AS [ReferringHospital],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransactionID] AS VARCHAR(MAX)) AS [TransactionID],
		CAST([TreatmentPeriod] AS VARCHAR(MAX)) AS [TreatmentPeriod] 
	FROM Intelligence.viewreader.vGVR_InpatientAdmissions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    