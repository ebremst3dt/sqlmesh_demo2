
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda undersökningar.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'Examination': 'varchar(max)', 'ExaminationCode': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'Group': 'varchar(max)', 'GroupID': 'varchar(max)', 'HasEKGRegistration': 'varchar(max)', 'IsActive': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'ExaminationID': "{'title_ui': 'Önskade undersökningar', 'description': 'Id för vald undersökning'}", 'ExaminationCode': "{'title_ui': 'Önskade undersökningar', 'description': 'RIS-kod för vald undersökning'}", 'Examination': "{'title_ui': 'Önskade undersökningar', 'description': 'Vald undersökning i klartext'}", 'HasEKGRegistration': "{'title_ui': None, 'description': 'EKG har registrerats'}", 'IsActive': "{'title_ui': None, 'description': 'Aktiv'}", 'GroupID': "{'title_ui': 'Grupperingar', 'description': 'Kod för vald grupp'}", 'Group': "{'title_ui': 'Grupperingar', 'description': 'Vald grupp i klartext'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Examination AS VARCHAR(MAX)) AS Examination,
		CAST(ExaminationCode AS VARCHAR(MAX)) AS ExaminationCode,
		CAST(ExaminationID AS VARCHAR(MAX)) AS ExaminationID,
		CAST(Group AS VARCHAR(MAX)) AS Group,
		CAST(GroupID AS VARCHAR(MAX)) AS GroupID,
		CAST(HasEKGRegistration AS VARCHAR(MAX)) AS HasEKGRegistration,
		CAST(IsActive AS VARCHAR(MAX)) AS IsActive,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPhysiologyOrders_Exams) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    