
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Kompletterande uppgifter (tidigare medicinsk information). Skapades av det gränssnitt som tidigare användes på Huddinge Sjukhus.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AntibioticsTherapy': 'varchar(max)', 'DiscontinuedDate': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EnteredDate': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'AntibioticsTherapy': "{'title_ui': 'Antibiotikaterapi', 'description': 'Antibiotikaterapi'}", 'EnteredDate': "{'title_ui': 'Insatt', 'description': 'Insatt'}", 'DiscontinuedDate': "{'title_ui': 'Utsatt', 'description': 'Utsatt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AntibioticsTherapy AS VARCHAR(MAX)) AS AntibioticsTherapy,
		CAST(DiscontinuedDate AS VARCHAR(MAX)) AS DiscontinuedDate,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(EnteredDate AS VARCHAR(MAX)) AS EnteredDate,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vBactLabOrders_MedInfo1) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    