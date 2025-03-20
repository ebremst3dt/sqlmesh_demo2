
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Skannade dokument. Endast metadata lagras här. Dokumenttypen versionshanteras ej.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AttesterUserID': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedByUser': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'ScannedDocID': 'varchar(max)', 'ScannedDocTypeTermID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'ScannedDocID': "{'title_ui': 'Ankomsttid', 'description': 'Tidsstämpel då skannat dokument skapats. Utgör id (nyckel) till dokumentet'}", 'SavedByUserID': "{'title_ui': 'Version skapad av/Makulerad av', 'description': 'PID för användare som skapat eller makulerat dokumentet'}", 'SavedByUser': "{'title_ui': 'Version skapad av/Makulerad av', 'description': 'Namn på användare som skapat eller makulerat dokumentet'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'PID för användare som skapat dokumentet'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör arbetsplats', 'description': 'Vårdenhets ID, original (dokumentet skapat på denna arb.plats). Styr behörigheter.'}", 'RegistrationStatusID': "{'title_ui': None, 'description': {'break': None}}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Synlig i översikten'}", 'AttesterUserID': "{'title_ui': 'Vidimeringsansvarig', 'description': 'Vidimeringsansvarig (original). Här syns det inte om vidimeringsansvarig ändrats efter att dokumentet sparats.'}", 'ScannedDocTypeTermID': "{'title_ui': 'Dokumenttyp', 'description': 'Dokumenttypens term-id'}", 'EventDate': "{'title_ui': 'Händelsetid/Datum/tid', 'description': 'Händelsedatum'}", 'EventTime': "{'title_ui': 'Händelsetid/Datum/tid', 'description': 'Händelsetid'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AttesterUserID AS VARCHAR(MAX)) AS AttesterUserID,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CONVERT(varchar(max), EventDate, 126) AS EventDate,
		CONVERT(varchar(max), EventTime, 126) AS EventTime,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(SavedByUser AS VARCHAR(MAX)) AS SavedByUser,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), ScannedDocID, 126) AS ScannedDocID,
		CAST(ScannedDocTypeTermID AS VARCHAR(MAX)) AS ScannedDocTypeTermID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vScannedDocuments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    