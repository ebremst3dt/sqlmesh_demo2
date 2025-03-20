
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Diagnoser",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ChronicEndDate': 'varchar(max)', 'ChronicEndTime': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DiagnosisDate': 'varchar(max)', 'DiagnosisID': 'varchar(max)', 'DiagnosisTime': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsCauseOfDeath': 'varchar(max)', 'IsChronic': 'varchar(max)', 'IsOnSigningList': 'varchar(max)', 'PatientID': 'varchar(max)', 'RegistrationStatus': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'RegistrationStatus': "{'title_ui': None, 'description': {'break': None}}", 'TimestampSaved': "{'title_ui': 'Sparad', 'description': 'Version skapad'}", 'SavedByUserID': "{'title_ui': 'Sparad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'TimestampCreated': "{'title_ui': 'Skapad', 'description': None}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': 'Original skapat av'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'DiagnosisID': "{'title_ui': None, 'description': 'UUID - Universally unique identifier (Microsofts) för diagnosen hos patienten. Ny 2007-01. Skapas då första version av diagnosen skapas.'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Signerad av'}", 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': None}", 'IsOnSigningList': "{'title_ui': 'Spara i signeringslista', 'description': 'Om diagnosen sparad i signeringslista'}", 'DiagnosisDate': '{\'title_ui\': \'Händelsedatum\', \'description\': \'T.o.m. nov 2010 stod datumet för "Avliden" då IsCauseOfDeath var True.\'}', 'DiagnosisTime': '{\'title_ui\': None, \'description\': \'Tid som ej används idag, minnesutrymme är avsatt och kopplat till "DiagnosisDate".\'}', 'IsChronic': "{'title_ui': 'Kronisk', 'description': 'Diagnosen visas som kronisk'}", 'ChronicEndDate': "{'title_ui': 'Avslutad', 'description': {'break': None}}", 'ChronicEndTime': '{\'title_ui\': None, \'description\': \'Tid som ej används idag, minnesutrymme är avsatt och kopplat till "ChronicEndDate".\'}', 'IsCauseOfDeath': "{'title_ui': 'Dödsorsak', 'description': 'Denna diagnos var dödsorsak'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), ChronicEndDate, 126) AS ChronicEndDate,
		CONVERT(varchar(max), ChronicEndTime, 126) AS ChronicEndTime,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CONVERT(varchar(max), DiagnosisDate, 126) AS DiagnosisDate,
		CAST(DiagnosisID AS VARCHAR(MAX)) AS DiagnosisID,
		CONVERT(varchar(max), DiagnosisTime, 126) AS DiagnosisTime,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(IsCauseOfDeath AS VARCHAR(MAX)) AS IsCauseOfDeath,
		CAST(IsChronic AS VARCHAR(MAX)) AS IsChronic,
		CAST(IsOnSigningList AS VARCHAR(MAX)) AS IsOnSigningList,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(RegistrationStatus AS VARCHAR(MAX)) AS RegistrationStatus,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CONVERT(varchar(max), SignedDatetime, 126) AS SignedDatetime,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CONVERT(varchar(max), TimestampCreated, 126) AS TimestampCreated,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vDiagnoses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    