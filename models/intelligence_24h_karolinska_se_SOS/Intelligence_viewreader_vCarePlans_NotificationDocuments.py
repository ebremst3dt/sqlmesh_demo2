
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Dokument som tillsammans utgör en kallelse",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DocumentName': 'varchar(max)', 'DocumentTypeID': 'varchar(max)', 'LetterTemplateID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PrintDatetime': 'varchar(max)', 'Row': 'varchar(max)', 'SomeUserName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': 'Vårdplansid', 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'DocumentTypeID': "{'title_ui': None, 'description': {'break': None}}", 'DocumentName': "{'title_ui': 'Dokument', 'description': 'Beteckning för dokumentet'}", 'LetterTemplateID': "{'title_ui': None, 'description': 'Id för brevmall'}", 'PrintDatetime': "{'title_ui': None, 'description': 'Utskriftsdatum. Verkar inte användas'}", 'SomeUserName': "{'title_ui': None, 'description': 'Den som skrivit ut? Verkar inte användas'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentName AS VARCHAR(MAX)) AS DocumentName,
		CAST(DocumentTypeID AS VARCHAR(MAX)) AS DocumentTypeID,
		CAST(LetterTemplateID AS VARCHAR(MAX)) AS LetterTemplateID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), PrintDatetime, 126) AS PrintDatetime,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SomeUserName AS VARCHAR(MAX)) AS SomeUserName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vCarePlans_NotificationDocuments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    