
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Medicinska patientuppgifter (vissa delar av modulen Patientuppgifter i TakeCare). I dagsläget påverkar inte användarens och dokumentets vårdenhet behörighet, utan alla har rätt att se allt data i denna tabell. Versionshanteras, men endast den senaste versionen visas upp.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'BirthWeight': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DeceasedDatetimeText': 'varchar(max)', 'DeceasedSavedAtCareUnitID': 'varchar(max)', 'DeceasedSavedByUserID': 'varchar(max)', 'DeceasedTimestampSaved': 'varchar(max)', 'PatientID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': None, 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': None, 'description': 'Den användare som senast har ändrat dokumentet'}", 'CreatedAtCareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet som data är skapat på'}", 'DeceasedDatetimeText': "{'title_ui': 'Avliden tid', 'description': 'Användarregistrerat datum då patient avlidit. Trots att patienten visas som avliden i TakeCare, lagras inte alltid data i denna kolumn.'}", 'BirthWeight': "{'title_ui': 'Födelsevikt', 'description': 'Patientens födelsevikt. Är ibland registrerat i gram, ibland i kilogram. Fältet i TakeCare kan stängas av i generella systemparametrar.'}", 'DeceasedTimestampSaved': "{'title_ui': 'Avliden registrerad', 'description': 'Datum då användare registrerat datum i Avliden tid'}", 'DeceasedSavedByUserID': "{'title_ui': 'Avliden registrerad av', 'description': 'Användare som registrerat datum i Avliden tid'}", 'DeceasedSavedAtCareUnitID': "{'title_ui': 'Avliden registrerad på', 'description': 'Vårdenhet som användaren registrerat datum i Avliden tid på'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(BirthWeight AS VARCHAR(MAX)) AS BirthWeight,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(DeceasedDatetimeText AS VARCHAR(MAX)) AS DeceasedDatetimeText,
		CAST(DeceasedSavedAtCareUnitID AS VARCHAR(MAX)) AS DeceasedSavedAtCareUnitID,
		CAST(DeceasedSavedByUserID AS VARCHAR(MAX)) AS DeceasedSavedByUserID,
		CONVERT(varchar(max), DeceasedTimestampSaved, 126) AS DeceasedTimestampSaved,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPatInfoMed) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    