
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Data som lagras per cell. Raderna hör ihop som en inmatning, där varje kolumn kan ses som en egenskap på det inmatade datat. Denna cell innehåller text.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ColumnHeadingTermID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'KeywordTermID': 'varchar(max)', 'Note': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'KeywordTermID': "{'title_ui': None, 'description': 'Term-id för detta sökord'}", 'ColumnHeadingTermID': "{'title_ui': None, 'description': 'Term-id för kolumnen'}", 'Row': "{'title_ui': None, 'description': 'Den rad i tabellen som denna cell tillhör'}", 'Note': "{'title_ui': None, 'description': 'Det inmatade värdet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(ColumnHeadingTermID AS VARCHAR(MAX)) AS ColumnHeadingTermID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(KeywordTermID AS VARCHAR(MAX)) AS KeywordTermID,
		CAST(Note AS VARCHAR(MAX)) AS Note,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vCaseNotes_ContainerNotes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    