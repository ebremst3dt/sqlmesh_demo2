
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Läkemedel - Administreringsvägar",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdministrationRouteID': 'varchar(max)', 'DoseForm': 'varchar(max)', 'IsInfusion': 'varchar(max)', 'IsOxygenTreatment': 'varchar(max)', 'Name': 'varchar(max)', 'ShortName': 'varchar(max)', 'SimpleName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'AdministrationRouteID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'ShortName': "{'title_ui': 'Namn, korttext', 'description': None}", 'IsInfusion': "{'title_ui': 'Tänder spädning', 'description': 'Om administrationsvägen används för infusion'}", 'DoseForm': "{'title_ui': 'Autoval läkemedelsform', 'description': 'Autoval läkemedelsformer Apoteket/SIL'}", 'SimpleName': "{'title_ui': 'Namn, dosanvisning', 'description': 'Förenklad text som används på recept till patienten.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AdministrationRouteID AS VARCHAR(MAX)) AS AdministrationRouteID,
		CAST(DoseForm AS VARCHAR(MAX)) AS DoseForm,
		CAST(IsInfusion AS VARCHAR(MAX)) AS IsInfusion,
		CAST(IsOxygenTreatment AS VARCHAR(MAX)) AS IsOxygenTreatment,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(ShortName AS VARCHAR(MAX)) AS ShortName,
		CAST(SimpleName AS VARCHAR(MAX)) AS SimpleName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_AdministrationRoutes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    