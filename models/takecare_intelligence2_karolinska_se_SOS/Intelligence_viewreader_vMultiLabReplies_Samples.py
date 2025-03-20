
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Prov som grupperar analyser",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DocumentID': 'varchar(max)', 'Group': 'varchar(max)', 'IsPairSample': 'varchar(max)', 'PatientID': 'varchar(max)', 'SampleComment': 'varchar(max)', 'SampleID': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SpecimenName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'SampleID': "{'title_ui': None, 'description': 'Labbets id på provet'}", 'Group': "{'title_ui': None, 'description': 'Grupp'}", 'SamplingDate': "{'title_ui': None, 'description': 'Datum då provtagning skett'}", 'SpecimenName': "{'title_ui': 'Provmaterial', 'description': None}", 'IsPairSample': '{\'title_ui\': None, \'description\': \'Om det är ett parallellsatt prov så visas texten "Tidigare analyserat prov från..."\'}', 'SampleComment': "{'title_ui': 'Provkommentar', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(IsPairSample AS VARCHAR(MAX)) AS IsPairSample,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(SampleComment AS VARCHAR(MAX)) AS SampleComment,
		CAST(SampleID AS VARCHAR(MAX)) AS SampleID,
		CONVERT(varchar(max), SamplingDate, 126) AS SamplingDate,
		CAST(SpecimenName AS VARCHAR(MAX)) AS SpecimenName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMultiLabReplies_Samples) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    