
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AdministrationRouteID': 'varchar(max)', 'DatabaseID': 'varchar(max)', 'DosageAmount': 'varchar(max)', 'DosageUnitID': 'varchar(max)', 'SpecialityID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'VaccinationLocalizationID': 'varchar(max)', 'VaccineTypeID': 'varchar(max)'},
    column_descriptions={'VaccineTypeID': "{'title_ui': 'Id', 'description': None}", 'SpecialityID': "{'title_ui': 'Spec-Id', 'description': 'Spec-id'}", 'DatabaseID': "{'title_ui': 'Databas-Id', 'description': 'Database-id'}", 'AdministrationRouteID': "{'title_ui': 'Administrationsväg-Id', 'description': 'Administrationsväg-Id'}", 'VaccinationLocalizationID': "{'title_ui': 'Lokalisations-Id', 'description': 'Lokalisations-Id'}", 'DosageAmount': "{'title_ui': 'Doseringsmängd', 'description': None}", 'DosageUnitID': "{'title_ui': 'Doseringsenhet', 'description': 'Kod för enhet för dosen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AdministrationRouteID] AS VARCHAR(MAX)) AS [AdministrationRouteID],
		CAST([DatabaseID] AS VARCHAR(MAX)) AS [DatabaseID],
		CAST([DosageAmount] AS VARCHAR(MAX)) AS [DosageAmount],
		CAST([DosageUnitID] AS VARCHAR(MAX)) AS [DosageUnitID],
		CAST([SpecialityID] AS VARCHAR(MAX)) AS [SpecialityID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([VaccinationLocalizationID] AS VARCHAR(MAX)) AS [VaccinationLocalizationID],
		CAST([VaccineTypeID] AS VARCHAR(MAX)) AS [VaccineTypeID] 
	FROM Intelligence.viewreader.vCodes_VaccineTypesDrugs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    