
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Används för att matcha beställda undersökningar mot besvarade. Innehåller ofta samma data som Exams men inte alltid""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'Examination': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'IsEditedByLab': 'varchar(max)', 'Method': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'StatusCode': 'varchar(max)', 'StatusComment': 'varchar(max)', 'StatusEventDateTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ExaminationID': "{'title_ui': None, 'description': 'Kod för vald undersökning'}", 'Examination': "{'title_ui': 'Beställda undersökningar', 'description': 'Vald undersökning i klartext'}", 'Method': "{'title_ui': None, 'description': 'Vald metod'}", 'StatusCode': "{'title_ui': 'Status', 'description': 'Statuskod'}", 'StatusEventDateTime': "{'title_ui': 'Tid', 'description': 'Händelsetid för det som statuskoden avser'}", 'StatusComment': "{'title_ui': 'Kommentar', 'description': 'Statuskommentar'}", 'IsEditedByLab': "{'title_ui': None, 'description': 'Undersökningen som beställts har ändrats av labb till att vara en annan.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Examination AS VARCHAR(MAX)) AS Examination,
		CAST(ExaminationID AS VARCHAR(MAX)) AS ExaminationID,
		CAST(IsEditedByLab AS VARCHAR(MAX)) AS IsEditedByLab,
		CAST(Method AS VARCHAR(MAX)) AS Method,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(StatusCode AS VARCHAR(MAX)) AS StatusCode,
		CAST(StatusComment AS VARCHAR(MAX)) AS StatusComment,
		CONVERT(varchar(max), StatusEventDateTime, 126) AS StatusEventDateTime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vRadiologyOrdersSectra_ExamMatching) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    