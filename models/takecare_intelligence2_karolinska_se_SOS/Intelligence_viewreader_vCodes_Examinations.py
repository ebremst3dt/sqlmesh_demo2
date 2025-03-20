
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Undersökningar som görs på en viss vårdenhet. Man kan koppla undersökningar till ett bokningsunderlag.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Body': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'Heading': 'varchar(max)', 'Name': 'varchar(max)', 'TaskTermID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'ExaminationID': "{'title_ui': None, 'description': None}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'Heading': "{'title_ui': 'Rubrik', 'description': 'Används inte för tillfället'}", 'Body': "{'title_ui': 'Brödtext', 'description': 'Kan stoppas in i kallelsen som går till patienten och därför innehålla taggar som ersätts med bl.a. den bokade tiden, eller patientens namn vid utskrift.'}", 'TaskTermID': '{\'title_ui\': \'Aktivitet\', \'description\': \'Om man vill kan man koppla en aktivitet till undersökningen. På så sätt kan man få aktiviteten att dyka upp i "Att göra" i TakeCare automatiskt.\'}', 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Body AS VARCHAR(MAX)) AS Body,
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CAST(ExaminationID AS VARCHAR(MAX)) AS ExaminationID,
		CAST(Heading AS VARCHAR(MAX)) AS Heading,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(TaskTermID AS VARCHAR(MAX)) AS TaskTermID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_Examinations) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    