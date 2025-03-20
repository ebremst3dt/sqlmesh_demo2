
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Formulär och blanketter.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'FormName': 'varchar(max)', 'FormReceiverID': 'varchar(max)', 'FormTemplateID': 'varchar(max)', 'FormTemplateVersion': 'varchar(max)', 'PatientID': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedBy': 'varchar(max)', 'SavedByRoleID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'UUID': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedBy': "{'title_ui': 'Version skapad av', 'description': 'Namn på användare som sparat dokumentet'}", 'SavedByRoleID': "{'title_ui': None, 'description': 'Roll-id på användaren som sparat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Version skapad på'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'Skapad av'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Lagras bara vid makulering eller ett felmeddelande vid fel i leverans, annars tomt.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'EventDate': "{'title_ui': 'Händelsedatum', 'description': 'Datum då vårdgivaren varit i kontakt med patienten'}", 'EventTime': "{'title_ui': 'Händelsedatum', 'description': 'Tid då vårdgivaren varit i kontakt med patienten'}", 'SignerUserID': "{'title_ui': None, 'description': 'Signeringsansvarig'}", 'FormTemplateID': "{'title_ui': 'Formulärid', 'description': 'Formulärdefinitionens id'}", 'FormName': "{'title_ui': 'Formulärnamn', 'description': 'Formuläret eller blankettens namn i klartext'}", 'FormTemplateVersion': "{'title_ui': None, 'description': 'Formulärdefinitionens version när första versionen av dokumentet skapades'}", 'UUID': "{'title_ui': None, 'description': 'Ett globalt unikt ID på dokumentet'}", 'FormReceiverID': "{'title_ui': 'Mottagare', 'description': 'Mottagare (endast aktuell om RegistrationStatusID = 3 och blanketten skickats elektroniskt med emu version 3.1)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CONVERT(varchar(max), EventDate, 126) AS EventDate,
		CONVERT(varchar(max), EventTime, 126) AS EventTime,
		CAST(FormName AS VARCHAR(MAX)) AS FormName,
		CAST(FormReceiverID AS VARCHAR(MAX)) AS FormReceiverID,
		CAST(FormTemplateID AS VARCHAR(MAX)) AS FormTemplateID,
		CAST(FormTemplateVersion AS VARCHAR(MAX)) AS FormTemplateVersion,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedBy AS VARCHAR(MAX)) AS SavedBy,
		CAST(SavedByRoleID AS VARCHAR(MAX)) AS SavedByRoleID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignedBy AS VARCHAR(MAX)) AS SignedBy,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CONVERT(varchar(max), SignedDatetime, 126) AS SignedDatetime,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(UUID AS VARCHAR(MAX)) AS UUID,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vForms) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    