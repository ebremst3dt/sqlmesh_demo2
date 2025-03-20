
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda undersökningar.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'Examination': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'Group': 'varchar(max)', 'GroupID': 'varchar(max)', 'IsRequired': 'varchar(max)', 'Method': 'varchar(max)', 'MethodID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Side': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ExaminationID': "{'title_ui': 'Undersökning', 'description': 'Kod för vald undersökning'}", 'Examination': "{'title_ui': 'Undersökning', 'description': 'Vald undersökning i klartext'}", 'MethodID': "{'title_ui': 'Metod', 'description': 'Kod för vald metod. Ingen metod vald=1.'}", 'Method': "{'title_ui': 'Metod', 'description': 'Vald metod i klartext'}", 'Side': "{'title_ui': 'Sida', 'description': 'På vilken sida av kroppen som undersökningen ska göras.'}", 'GroupID': "{'title_ui': None, 'description': 'Kod för vald grupp'}", 'Group': "{'title_ui': None, 'description': 'Vald grupp i klartext'}", 'IsRequired': "{'title_ui': None, 'description': 'Analysen är obligatorisk'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Group AS VARCHAR(MAX)) AS Group,
		CAST(GroupID AS VARCHAR(MAX)) AS GroupID,
		CAST(IsRequired AS VARCHAR(MAX)) AS IsRequired,
		CAST(Method AS VARCHAR(MAX)) AS Method,
		CAST(MethodID AS VARCHAR(MAX)) AS MethodID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(Side AS VARCHAR(MAX)) AS Side,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vRadiologyOrdersSectra_Exams) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    