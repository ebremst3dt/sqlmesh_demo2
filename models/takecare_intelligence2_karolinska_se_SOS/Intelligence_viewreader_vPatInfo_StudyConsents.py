
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Medgivanden till medverkan i studier""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DecidedByUserID': 'varchar(max)', 'DecidedByUserName': 'varchar(max)', 'DecidedDate': 'varchar(max)', 'HasLeft': 'varchar(max)', 'ObtainedByUserID': 'varchar(max)', 'ObtainedByUserName': 'varchar(max)', 'ObtainedDate': 'varchar(max)', 'PatientID': 'varchar(max)', 'PatientStudyID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'Status': 'varchar(max)', 'StudyID': 'varchar(max)', 'StudyLeaveDate': 'varchar(max)', 'StudyLeaveReasonID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'StudyID': "{'title_ui': 'Studie', 'description': 'Identifierar den studie det gäller'}", 'PatientStudyID': "{'title_ui': 'PatientID i studien', 'description': 'Patientens id i studien'}", 'HasLeft': "{'title_ui': 'Utträde', 'description': 'Om patienten har utträtt ur studien innan den avslutats'}", 'StudyLeaveReasonID': "{'title_ui': 'Orsak till utträde', 'description': 'Anledning till varför patienten begärt utträde'}", 'StudyLeaveDate': "{'title_ui': 'Utträdesdatum', 'description': 'Datum då patienten begärt utträde'}", 'ObtainedByUserID': "{'title_ui': 'Inhämtat av', 'description': 'Den användare som inhämtat patientens samtycke (fr.o.m. sommaren 2007)'}", 'ObtainedByUserName': "{'title_ui': 'Inhämtat av', 'description': 'Den användare som inhämtat patientens samtycke (användarnamn)'}", 'ObtainedDate': "{'title_ui': 'Datum inhämtat', 'description': None}", 'DecidedByUserID': "{'title_ui': 'Ansvarig för beslut', 'description': 'Den användare som fattat beslutet (fr.o.m. sommaren 2007)'}", 'DecidedByUserName': "{'title_ui': 'Ansvarig för beslut', 'description': 'Den användare som fattat beslutet (användarnamn)'}", 'DecidedDate': "{'title_ui': 'Datum beslut', 'description': None}", 'TimestampSaved': "{'title_ui': 'Registrerad', 'description': 'Tidpunkten då samtycket registrerades i systemet'}", 'SavedByUserID': "{'title_ui': 'Registrerad av', 'description': 'Den användare som registrerat samtycket i systemet'}", 'SavedAtCareUnitID': "{'title_ui': 'Registrerad på', 'description': 'Där data är registrerat'}", 'Status': "{'title_ui': None, 'description': 'Status för samtycket där 0 = Normal och -2 = Makulerad'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DecidedByUserID AS VARCHAR(MAX)) AS DecidedByUserID,
		CAST(DecidedByUserName AS VARCHAR(MAX)) AS DecidedByUserName,
		CONVERT(varchar(max), DecidedDate, 126) AS DecidedDate,
		CAST(HasLeft AS VARCHAR(MAX)) AS HasLeft,
		CAST(ObtainedByUserID AS VARCHAR(MAX)) AS ObtainedByUserID,
		CAST(ObtainedByUserName AS VARCHAR(MAX)) AS ObtainedByUserName,
		CONVERT(varchar(max), ObtainedDate, 126) AS ObtainedDate,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PatientStudyID AS VARCHAR(MAX)) AS PatientStudyID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(Status AS VARCHAR(MAX)) AS Status,
		CAST(StudyID AS VARCHAR(MAX)) AS StudyID,
		CONVERT(varchar(max), StudyLeaveDate, 126) AS StudyLeaveDate,
		CAST(StudyLeaveReasonID AS VARCHAR(MAX)) AS StudyLeaveReasonID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved 
	FROM Intelligence.viewreader.vPatInfo_StudyConsents) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    