
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kopplade dokument.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DocumentTypeID': 'varchar(max)', 'LinkedDocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferenceType': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Är alltid 1 för att möjliggöra främmande nyckel till huvudtabellen.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'LinkedDocumentID': "{'title_ui': None, 'description': 'Id för länkat dokument'}", 'ReferenceType': "{'title_ui': None, 'description': 'Typ av post, t.ex. PASType eller id för dokumenttyp'}", 'DocumentTypeID': "{'title_ui': None, 'description': 'Id för dokumenttyp'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentTypeID AS VARCHAR(MAX)) AS DocumentTypeID,
		CAST(LinkedDocumentID AS VARCHAR(MAX)) AS LinkedDocumentID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReferenceType AS VARCHAR(MAX)) AS ReferenceType,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vCaseNotes_LinkedDocuments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    