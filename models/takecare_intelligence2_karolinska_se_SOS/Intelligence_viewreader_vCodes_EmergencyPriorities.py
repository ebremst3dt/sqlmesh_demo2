
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Prioritetsklassning som används för patienter på akuten.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ColorBlue': 'varchar(max)', 'ColorGreen': 'varchar(max)', 'ColorRed': 'varchar(max)', 'IsActive': 'varchar(max)', 'MaxWaitTime': 'varchar(max)', 'Name': 'varchar(max)', 'PriorityID': 'varchar(max)', 'ShortName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'PriorityID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': 'Namn', 'description': 'Namn på prioritetsklassen'}", 'MaxWaitTime': "{'title_ui': 'Tid för omhändertagande', 'description': 'Inom hur lång tid patienter som hör till denna klass måste tas om hand. Enhet är minuter'}", 'IsActive': "{'title_ui': 'Giltig t.o.m.', 'description': 'Om denna klass används för tillfället.'}", 'ShortName': "{'title_ui': 'Kortnamn', 'description': 'Ett kortare namn som visas i akutliggaren'}", 'ColorRed': "{'title_ui': 'Färgkod', 'description': 'Den färg (i RGB) som ska användas för denna prioritet i akutliggaren. Om standardfärg används är denna kolumn NULL.'}", 'ColorGreen': "{'title_ui': 'Färgkod', 'description': 'Den färg (i RGB) som ska användas för denna prioritet i akutliggaren. Om standardfärg används är denna kolumn NULL.'}", 'ColorBlue': "{'title_ui': 'Färgkod', 'description': 'Den färg (i RGB) som ska användas för denna prioritet i akutliggaren. Om standardfärg används är denna kolumn NULL.'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då klassen är giltig'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(ColorBlue AS VARCHAR(MAX)) AS ColorBlue,
		CAST(ColorGreen AS VARCHAR(MAX)) AS ColorGreen,
		CAST(ColorRed AS VARCHAR(MAX)) AS ColorRed,
		CAST(IsActive AS VARCHAR(MAX)) AS IsActive,
		CAST(MaxWaitTime AS VARCHAR(MAX)) AS MaxWaitTime,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(PriorityID AS VARCHAR(MAX)) AS PriorityID,
		CAST(ShortName AS VARCHAR(MAX)) AS ShortName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_EmergencyPriorities) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    