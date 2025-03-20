
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Allmän och övergripande data för varje journalanteckning. En anteckning kan innehålla flera sökord och varje sökord kan ha olika datatyper.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CaseNoteUUID': 'varchar(max)', 'CounterSignerUserID': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventTime': 'varchar(max)', 'IsSigned': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': 'Tidpunkt för påbörjan av dokumentets ursprungliga version (ny anteckning öppnad). För anteckningar från gamla Dolly-systemet saknas klockslag, och är satt till 00:00 (gäller endast Karolinska).'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Användaren som skapade den första versionen'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör arbetsplats', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Yrkesgrupp för den valda journaltextmallen.'}", 'EventDate': "{'title_ui': 'Händelsetid', 'description': 'Datum som journalanteckningen avser (exempelvis för patientbesök)'}", 'EventTime': "{'title_ui': 'Händelsetid', 'description': 'Klockslag som journalanteckningen avser'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering. För anteckningar från gamla Dolly-systemet saknas klockslag, och är satt till 00:00 (gäller endast Karolinska).'}", 'IsSigned': "{'title_ui': None, 'description': 'Om anteckningen är signerad. Nytt fält i Intelligence 2009-06.'}", 'SignedByUserID': '{\'title_ui\': \'Signerad av\', \'description\': \'Användaren som signerat eller kontrasignerat dokumentet. En kontrasignatur skriver över en "vanlig" signatur. Alla gamla anteckningar har inte lagrat vem som signerade dokumentet.\'}', 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': 'Användaren som är ansvarig för att signera dokumentet'}", 'CounterSignerUserID': "{'title_ui': 'Kontrasigneringsansvarig', 'description': 'Användaren som är ansvarig för att kontrasignera dokumentet'}", 'CaseNoteUUID': "{'title_ui': None, 'description': 'UUID. Kan sättas av ett externt system.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CaseNoteUUID AS VARCHAR(MAX)) AS CaseNoteUUID,
		CAST(CounterSignerUserID AS VARCHAR(MAX)) AS CounterSignerUserID,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CONVERT(varchar(max), EventDate, 126) AS EventDate,
		CONVERT(varchar(max), EventTime, 126) AS EventTime,
		CAST(IsSigned AS VARCHAR(MAX)) AS IsSigned,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ProfessionID AS VARCHAR(MAX)) AS ProfessionID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CONVERT(varchar(max), SignedDatetime, 126) AS SignedDatetime,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vCaseNotes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    