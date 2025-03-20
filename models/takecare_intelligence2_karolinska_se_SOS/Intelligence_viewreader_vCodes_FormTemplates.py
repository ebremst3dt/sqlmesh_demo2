
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Formulärdefinitioner (Design av formulär och blanketter)",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Description': 'varchar(max)', 'EDISettingID': 'varchar(max)', 'FormName': 'varchar(max)', 'FormTemplateID': 'varchar(max)', 'HasToBeSent': 'varchar(max)', 'HasToBeSigned': 'varchar(max)', 'IsGroupEditable': 'varchar(max)', 'IsLinkedDocsEnabled': 'varchar(max)', 'IsPublished': 'varchar(max)', 'IsWatchEnabled': 'varchar(max)', 'LatestVersion': 'varchar(max)', 'SavedBy': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'StorageType': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'FormTemplateID': "{'title_ui': 'Identitet', 'description': 'Formulärdefinitionens id'}", 'LatestVersion': "{'title_ui': 'Version', 'description': 'Senast lagrade version'}", 'FormName': "{'title_ui': 'Namn', 'description': None}", 'Description': "{'title_ui': 'Beskrivning', 'description': None}", 'IsPublished': "{'title_ui': 'Publicerad', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': None}", 'TimestampSaved': "{'title_ui': 'Senast sparad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedBy': "{'title_ui': 'Senast sparad av', 'description': 'Namn på användare'}", 'SavedByUserID': "{'title_ui': 'Senast sparad av', 'description': 'Pnr för användare som sparat designen'}", 'TimestampCreated': "{'title_ui': None, 'description': None}", 'IsGroupEditable': "{'title_ui': 'Redigerbar inom', 'description': {'break': None}}", 'HasToBeSigned': "{'title_ui': 'Signeras', 'description': 'Om blanketten ska signeras'}", 'HasToBeSent': "{'title_ui': 'Skickas elektroniskt', 'description': 'Om blanketten ska skickas'}", 'EDISettingID': "{'title_ui': None, 'description': 'EDI-inställningen som används för denna blankett när den skickas'}", 'IsWatchEnabled': "{'title_ui': 'Bevakning möjlig', 'description': 'Om blanketten kan bevakas'}", 'IsLinkedDocsEnabled': "{'title_ui': 'Koppla dokument', 'description': 'Om blanketten ska ha möjlighet att ha koppla kopplade dokument'}", 'StorageType': "{'title_ui': 'Endast utskrift (kan ej sparas)', 'description': {'break': None}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CAST(EDISettingID AS VARCHAR(MAX)) AS EDISettingID,
		CAST(FormName AS VARCHAR(MAX)) AS FormName,
		CAST(FormTemplateID AS VARCHAR(MAX)) AS FormTemplateID,
		CAST(HasToBeSent AS VARCHAR(MAX)) AS HasToBeSent,
		CAST(HasToBeSigned AS VARCHAR(MAX)) AS HasToBeSigned,
		CAST(IsGroupEditable AS VARCHAR(MAX)) AS IsGroupEditable,
		CAST(IsLinkedDocsEnabled AS VARCHAR(MAX)) AS IsLinkedDocsEnabled,
		CAST(IsPublished AS VARCHAR(MAX)) AS IsPublished,
		CAST(IsWatchEnabled AS VARCHAR(MAX)) AS IsWatchEnabled,
		CAST(LatestVersion AS VARCHAR(MAX)) AS LatestVersion,
		CAST(SavedBy AS VARCHAR(MAX)) AS SavedBy,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(StorageType AS VARCHAR(MAX)) AS StorageType,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_FormTemplates) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    