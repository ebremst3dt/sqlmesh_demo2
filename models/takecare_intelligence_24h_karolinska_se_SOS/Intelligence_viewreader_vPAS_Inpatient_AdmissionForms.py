
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Psykiatrisk vårdform beskriver inskrivning (används inom psykiatrin). Kan göras om flera gånger under ett vårdtillfälle.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdmissionFormID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EventDatetime': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedByUserName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'SavedByUserName': "{'title_ui': 'Anv.namn', 'description': 'Den person som registrerat data (användarnamn)'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tid då psykiatrisk vårdform registrerats'}", 'AdmissionFormID': "{'title_ui': 'Psykiatrisk vårdform', 'description': 'Kod för psykiatrisk vårdform'}", 'EventDatetime': "{'title_ui': 'Beslutstid', 'description': 'Tid då psykiatrisk vårdform börjar gälla'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AdmissionFormID AS VARCHAR(MAX)) AS AdmissionFormID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CONVERT(varchar(max), EventDatetime, 126) AS EventDatetime,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedByUserName AS VARCHAR(MAX)) AS SavedByUserName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved 
	FROM Intelligence.viewreader.vPAS_Inpatient_AdmissionForms) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    