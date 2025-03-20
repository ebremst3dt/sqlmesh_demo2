
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Bilder eller video. Endast metadata lagras här. Dokumenttypen versionshanteras ej.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AttesterUserID': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'ImageID': 'varchar(max)', 'ImageType': 'varchar(max)', 'ImageTypeTermID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivingCareUnitID': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedByUser': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'ImageID': "{'title_ui': 'Ankomsttid', 'description': 'Tidsstämpel då bild sparats eller ankommit till mottagande vårdenhet. Utgör id (nyckel) till dokumentet'}", 'SavedByUserID': "{'title_ui': 'Version skapad av/Makulerad av', 'description': 'PID för användare som skapat eller makulerat dokumentet'}", 'SavedByUser': "{'title_ui': 'Version skapad av/Makulerad av', 'description': 'Namn på användare som skapat eller makulerat dokumentet'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'PID för användare som skapat dokumentet'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör arbetsplats', 'description': 'Vårdenhets ID, original (dokumentet skapat på denna arb.plats). Styr behörigheter.'}", 'RegistrationStatusID': "{'title_ui': None, 'description': {'break': None}}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Synlig i översikten'}", 'AttesterUserID': "{'title_ui': 'Vidimeringsansvarig', 'description': 'Vidimeringsansvarig (original). Här syns det inte om vidimeringsansvarig ändrats efter att dokumentet sparats.'}", 'ReceivingCareUnitID': "{'title_ui': 'Till vårdenhet/Mottagande vårdenhet/Vårdenhet', 'description': 'Mottagarens vårdenhets-id'}", 'ImageType': "{'title_ui': None, 'description': {'break': None}}", 'ImageTypeTermID': "{'title_ui': 'Bildtyp', 'description': 'Bildtypens term-id'}", 'EventDate': "{'title_ui': 'Händelsetid/Datum/tid', 'description': 'Händelsedatum'}", 'EventTime': "{'title_ui': 'Händelsetid/Datum/tid', 'description': 'Händelsetid'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), ImageID, 126) AS ImageID,
		CAST(ImageType AS VARCHAR(MAX)) AS ImageType,
		CAST(ImageTypeTermID AS VARCHAR(MAX)) AS ImageTypeTermID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReceivingCareUnitID AS VARCHAR(MAX)) AS ReceivingCareUnitID,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(SavedByUser AS VARCHAR(MAX)) AS SavedByUser,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vImages) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    