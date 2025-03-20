
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Paket av analyser som kan kopplas till en beställning (MultiLabb analyskatalog)",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'InvestigationSampleDemand': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'PackageExaminationCode': 'varchar(max)', 'PackageID': 'varchar(max)', 'PackageName': 'varchar(max)', 'ProfileCode': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'PackageID': "{'title_ui': None, 'description': 'Paketid'}", 'PackageName': "{'title_ui': None, 'description': 'Paketnamn'}", 'ProfileCode': "{'title_ui': None, 'description': 'Profilkod'}", 'PackageExaminationCode': "{'title_ui': None, 'description': 'Undersökningskod. Har ingen koppling till Codes_MultiLabExaminations'}", 'InvestigationSampleDemand': "{'title_ui': None, 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily"
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
		CAST(InvestigationSampleDemand AS VARCHAR(MAX)) AS InvestigationSampleDemand,
		CAST(OrderRegistryFileName AS VARCHAR(MAX)) AS OrderRegistryFileName,
		CAST(PackageExaminationCode AS VARCHAR(MAX)) AS PackageExaminationCode,
		CAST(PackageID AS VARCHAR(MAX)) AS PackageID,
		CAST(PackageName AS VARCHAR(MAX)) AS PackageName,
		CAST(ProfileCode AS VARCHAR(MAX)) AS ProfileCode,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_MultiLabPackages) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    