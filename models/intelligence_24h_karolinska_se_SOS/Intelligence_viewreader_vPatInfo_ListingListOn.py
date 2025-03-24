
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Listningsuppgifter från systemet ListOn. Används bara i Stockholm.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BVCCreatedDate': 'varchar(max)', 'BVCKombika': 'varchar(max)', 'BVCName': 'varchar(max)', 'BVCType': 'varchar(max)', 'CareProviderCode': 'varchar(max)', 'CareProviderName': 'varchar(max)', 'CreatedDate': 'varchar(max)', 'District': 'varchar(max)', 'LeaveDate': 'varchar(max)', 'LeaveReason': 'varchar(max)', 'MedicalCenter': 'varchar(max)', 'MedicalCenterKombika': 'varchar(max)', 'NursingHomeAdmissionDate': 'varchar(max)', 'NursingHomeCode': 'varchar(max)', 'NursingHomeName': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReadDate': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'ReadDate': "{'title_ui': 'Listningsuppgifter uppdaterade', 'description': 'Datum då informationen hämtats från listningssystem'}", 'District': "{'title_ui': 'Stadsdelsnamn', 'description': 'Namn på den stadsdel personen är bosatt i'}", 'Type': "{'title_ui': None, 'description': {'break': [None, None]}}", 'CareProviderCode': "{'title_ui': 'Husläkare, kod', 'description': 'Kod för husläkarteam eller kombika för husläkarmottagning, beroende på typ'}", 'CareProviderName': "{'title_ui': 'Husläkare, namn', 'description': 'Namn på husläkarteam eller husläkarmottagning'}", 'MedicalCenterKombika': "{'title_ui': 'Vårdcentral, kod', 'description': 'Kod för vårdenhet som vårdgivare tillhör'}", 'MedicalCenter': "{'title_ui': 'Vårdcentral, namn', 'description': 'Namn på vårdenhet som vårdgivare tillhör'}", 'CreatedDate': "{'title_ui': 'Anmälningsdatum', 'description': 'Listningens anmälningsdatum'}", 'LeaveDate': "{'title_ui': None, 'description': 'Det datum personnumret blev avregistrerat i ListOn'}", 'LeaveReason': "{'title_ui': None, 'description': 'Avregistreringsorsak'}", 'BVCType': "{'title_ui': None, 'description': 'BVC-listning om sådan finns'}", 'BVCKombika': "{'title_ui': 'BVC, kod', 'description': 'Kod för BVC'}", 'BVCName': "{'title_ui': 'BVC, namn', 'description': 'Namn på BVC'}", 'BVCCreatedDate': "{'title_ui': 'BVC, anmälningsdatum', 'description': 'BVC-listningens anmälningsdatum'}", 'NursingHomeAdmissionDate': "{'title_ui': 'Särskilt boende, inskrivningsdatum', 'description': 'Inskrivningsdatum på särskilt boende'}", 'NursingHomeCode': "{'title_ui': 'Särskilt boende, kod', 'description': 'Kombika eller HSAID för särskilt boende'}", 'NursingHomeName': "{'title_ui': 'Särskilt boende, namn', 'description': 'Namn på särskilt boende'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BVCCreatedDate] AS VARCHAR(MAX)) AS [BVCCreatedDate],
		CAST([BVCKombika] AS VARCHAR(MAX)) AS [BVCKombika],
		CAST([BVCName] AS VARCHAR(MAX)) AS [BVCName],
		CAST([BVCType] AS VARCHAR(MAX)) AS [BVCType],
		CAST([CareProviderCode] AS VARCHAR(MAX)) AS [CareProviderCode],
		CAST([CareProviderName] AS VARCHAR(MAX)) AS [CareProviderName],
		CAST([CreatedDate] AS VARCHAR(MAX)) AS [CreatedDate],
		CAST([District] AS VARCHAR(MAX)) AS [District],
		CAST([LeaveDate] AS VARCHAR(MAX)) AS [LeaveDate],
		CAST([LeaveReason] AS VARCHAR(MAX)) AS [LeaveReason],
		CAST([MedicalCenter] AS VARCHAR(MAX)) AS [MedicalCenter],
		CAST([MedicalCenterKombika] AS VARCHAR(MAX)) AS [MedicalCenterKombika],
		CONVERT(varchar(max), [NursingHomeAdmissionDate], 126) AS [NursingHomeAdmissionDate],
		CAST([NursingHomeCode] AS VARCHAR(MAX)) AS [NursingHomeCode],
		CAST([NursingHomeName] AS VARCHAR(MAX)) AS [NursingHomeName],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [ReadDate], 126) AS [ReadDate],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Type] AS VARCHAR(MAX)) AS [Type] 
	FROM Intelligence.viewreader.vPatInfo_ListingListOn) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    