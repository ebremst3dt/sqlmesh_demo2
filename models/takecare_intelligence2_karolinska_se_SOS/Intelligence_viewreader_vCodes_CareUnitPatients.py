
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Vårdenhetens lista i Sök/välj patient över patienter och den/de grupper patienten tillhör. Ögonblicksbild då data extraheras från journalsystemet.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AddedByUserID': 'varchar(max)', 'AddedDate': 'varchar(max)', 'AddedTime': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'GroupRow': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'CreatedAtCareUnitID': "{'title_ui': None, 'description': 'Vårdenhet patientlistan finns på'}", 'GroupRow': "{'title_ui': None, 'description': 'Internt löpnummer/id för den grupp på vårdenheten patienten lagts till i. Patient kan finnas i flera grupper inkl: 0=<Ej grupperade>.'}", 'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'AddedDate': "{'title_ui': 'Tillagd datum', 'description': 'Datum patienten lades till gruppen'}", 'AddedTime': "{'title_ui': 'Tillagd datum', 'description': 'Klockslag patienten lades till gruppen'}", 'AddedByUserID': "{'title_ui': 'Tillagd av', 'description': 'Användare som lade till patienten i gruppen'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Gruppspecifik patientkommentar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AddedByUserID AS VARCHAR(MAX)) AS AddedByUserID,
		CONVERT(varchar(max), AddedDate, 126) AS AddedDate,
		CONVERT(varchar(max), AddedTime, 126) AS AddedTime,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(GroupRow AS VARCHAR(MAX)) AS GroupRow,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_CareUnitPatients) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    