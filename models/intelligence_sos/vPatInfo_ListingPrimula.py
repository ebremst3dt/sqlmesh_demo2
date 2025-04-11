
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Listningsuppgifter från systemet Primula. Används bara på Gotland.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Active': 'varchar(max)', 'ChangeDate': 'varchar(max)', 'CreatedDate': 'varchar(max)', 'DoctorCode': 'varchar(max)', 'DoctorFirstName': 'varchar(max)', 'DoctorSurname': 'varchar(max)', 'ListingID': 'varchar(max)', 'MedicalCenter': 'varchar(max)', 'MedicalCenterCode': 'varchar(max)', 'MedicalCenterType': 'varchar(max)', 'MedicalCenterTypeFirstLetter': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReadDate': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'ListingID': "{'title_ui': 'Listnings ID', 'description': 'Id för listan som personen ligger på i listningssystemet'}", 'ReadDate': "{'title_ui': 'Listningsuppgifter uppdaterade', 'description': 'Datum då informationen hämtats från listningssystem'}", 'DoctorCode': "{'title_ui': 'Husläkarkod', 'description': None}", 'DoctorSurname': "{'title_ui': 'Läkare, efternamn', 'description': None}", 'DoctorFirstName': "{'title_ui': 'Läkare, förnamn', 'description': None}", 'MedicalCenterCode': "{'title_ui': 'Vårdcentralkod', 'description': None}", 'MedicalCenter': "{'title_ui': 'Vårdcentralnamn', 'description': None}", 'MedicalCenterType': "{'title_ui': 'Vårdcentraltyp', 'description': None}", 'MedicalCenterTypeFirstLetter': "{'title_ui': None, 'description': 'Första bokstaven i vårdcentraltypen'}", 'CreatedDate': "{'title_ui': 'Indatum', 'description': 'Inläggningsdatum'}", 'ChangeDate': "{'title_ui': 'Ändringsdatum', 'description': None}", 'Active': '{\'title_ui\': \'Listningstyp\', \'description\': \'"AKTIV" om aktiv\'}', 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['PatientID']
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
		CAST([Active] AS VARCHAR(MAX)) AS [Active],
		CAST([ChangeDate] AS VARCHAR(MAX)) AS [ChangeDate],
		CAST([CreatedDate] AS VARCHAR(MAX)) AS [CreatedDate],
		CAST([DoctorCode] AS VARCHAR(MAX)) AS [DoctorCode],
		CAST([DoctorFirstName] AS VARCHAR(MAX)) AS [DoctorFirstName],
		CAST([DoctorSurname] AS VARCHAR(MAX)) AS [DoctorSurname],
		CAST([ListingID] AS VARCHAR(MAX)) AS [ListingID],
		CAST([MedicalCenter] AS VARCHAR(MAX)) AS [MedicalCenter],
		CAST([MedicalCenterCode] AS VARCHAR(MAX)) AS [MedicalCenterCode],
		CAST([MedicalCenterType] AS VARCHAR(MAX)) AS [MedicalCenterType],
		CAST([MedicalCenterTypeFirstLetter] AS VARCHAR(MAX)) AS [MedicalCenterTypeFirstLetter],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [ReadDate], 126) AS [ReadDate],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vPatInfo_ListingPrimula) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    