
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Medicinsk varningsdata (ex. överkänslighet)""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AlertCode': 'varchar(max)', 'AlertTypeID': 'varchar(max)', 'Cause': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'Description': 'varchar(max)', 'EventTimeText': 'varchar(max)', 'HypersensitivityATCID': 'varchar(max)', 'HypersensitivityPreparationName': 'varchar(max)', 'HypersensitivitySeverityID': 'varchar(max)', 'HypersensitivityText': 'varchar(max)', 'HypersensitivityTextATC': 'varchar(max)', 'HypersensitivityTypeID': 'varchar(max)', 'IsCurrent': 'varchar(max)', 'PatientID': 'varchar(max)', 'Reference': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnit': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Användare som registrerat denna version av uppgifterna'}", 'SavedAtCareUnit': "{'title_ui': 'Senast ändrad på', 'description': 'Den vårdenhet där användaren befann sig när denna version av uppgifterna registrerades'}", 'TimestampCreated': "{'title_ui': 'Registrerad', 'description': 'Tidpunkt då uppgifterna skapades'}", 'CreatedByUserID': "{'title_ui': 'Registrerad av', 'description': 'Användare som skapade första versionen av uppgifterna'}", 'CreatedAtCareUnitID': "{'title_ui': 'Registrerad på', 'description': 'Den vårdenhet där första versionen av uppgifterna är skapade.'}", 'EventTimeText': "{'title_ui': 'Händelsetid', 'description': 'Då överkänsligheten upptäcktes'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar till varningen'}", 'IsCurrent': "{'title_ui': 'Aktuell', 'description': 'Om uppgiften fortfarande är aktuell'}", 'Cause': "{'title_ui': 'Orsak', 'description': 'Anledning till varför varningen är inaktuell'}", 'Reference': "{'title_ui': 'Hänvisning', 'description': 'Hänvisning till journalanteckning för mer information'}", 'Description': "{'title_ui': 'Beskrivning/ATC-text', 'description': 'Beskrivning inmatad eller genererad utifrån ex. diagnos- eller ATC-kod'}", 'AlertTypeID': "{'title_ui': 'Typ', 'description': 'Kod för varningstyp'}", 'HypersensitivityTypeID': "{'title_ui': 'Typ av överkänslighet', 'description': {'break': [None, None, None, None]}}", 'HypersensitivityPreparationName': "{'title_ui': 'Preparat', 'description': 'Preparatets namn (Apotekets/SILs interna namn)'}", 'HypersensitivityTextATC': "{'title_ui': 'ATC-text', 'description': 'Texten som hör till vald ATC-kod, hämtat från SIL, kan ej ändras av användaren'}", 'HypersensitivitySeverityID': "{'title_ui': 'Allvarlighet', 'description': 'Allvarlighetsgrad för överkänslighet'}", 'HypersensitivityText': "{'title_ui': 'Överkänslighet mot', 'description': 'Användarinmatad beskrivning av överkänslighet. Används som beskrivning av överkänslighet.'}", 'HypersensitivityATCID': "{'title_ui': 'ATC-kod', 'description': 'ATC-kod vid överkänslighet (fritext)'}", 'AlertCode': "{'title_ui': 'Multires. bakt/Diagnos/Behandling/Vårdbegränsning', 'description': 'Kod för bakterier, diagnos, behandling att beakta eller vårdbegränsning. Främmande nyckel beror på varningstyp. Är 1 om blodsmitta.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AlertCode AS VARCHAR(MAX)) AS AlertCode,
		CAST(AlertTypeID AS VARCHAR(MAX)) AS AlertTypeID,
		CAST(Cause AS VARCHAR(MAX)) AS Cause,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CAST(EventTimeText AS VARCHAR(MAX)) AS EventTimeText,
		CAST(HypersensitivityATCID AS VARCHAR(MAX)) AS HypersensitivityATCID,
		CAST(HypersensitivityPreparationName AS VARCHAR(MAX)) AS HypersensitivityPreparationName,
		CAST(HypersensitivitySeverityID AS VARCHAR(MAX)) AS HypersensitivitySeverityID,
		CAST(HypersensitivityText AS VARCHAR(MAX)) AS HypersensitivityText,
		CAST(HypersensitivityTextATC AS VARCHAR(MAX)) AS HypersensitivityTextATC,
		CAST(HypersensitivityTypeID AS VARCHAR(MAX)) AS HypersensitivityTypeID,
		CAST(IsCurrent AS VARCHAR(MAX)) AS IsCurrent,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Reference AS VARCHAR(MAX)) AS Reference,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedAtCareUnit AS VARCHAR(MAX)) AS SavedAtCareUnit,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPatInfoMed_Alerts) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    