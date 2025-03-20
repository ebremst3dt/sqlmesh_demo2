
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Grupper av analyser.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'Group': 'varchar(max)', 'GroupComment': 'varchar(max)', 'GroupID': 'varchar(max)', 'GroupRow': 'varchar(max)', 'MedicalComment': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'GroupRow': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'GroupID': "{'title_ui': None, 'description': 'Kod för gruppen'}", 'Group': "{'title_ui': None, 'description': 'Namn/beteckning för gruppen'}", 'MedicalComment': "{'title_ui': 'Medicinsk bedömning', 'description': 'Gemensam bedömning för hela gruppen'}", 'GroupComment': "{'title_ui': 'Laboratorie kommentar', 'description': 'Gemensam kommentar för hela gruppen'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Group AS VARCHAR(MAX)) AS Group,
		CAST(GroupComment AS VARCHAR(MAX)) AS GroupComment,
		CAST(GroupID AS VARCHAR(MAX)) AS GroupID,
		CAST(GroupRow AS VARCHAR(MAX)) AS GroupRow,
		CAST(MedicalComment AS VARCHAR(MAX)) AS MedicalComment,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPharmacologyReplies_Groups) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    