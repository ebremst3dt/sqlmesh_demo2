
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Sjukdomsbilder med tillhörande provmaterial (Virologlabb)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ClinicalPicture': 'varchar(max)', 'ClinicalPictureID': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'SpecimenID': "{'title_ui': None, 'description': 'Provmaterial som visas under vald sjukdomsbild.'}", 'ClinicalPictureID': "{'title_ui': None, 'description': 'Sjukdomsbildsid.'}", 'ClinicalPicture': "{'title_ui': None, 'description': 'Sjukdomsbild'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['ClinicalPictureID', 'SpecimenID']
    ),
    cron="@daily",
    start=start,
    enabled=True
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
		CAST([ClinicalPicture] AS VARCHAR(MAX)) AS [ClinicalPicture],
		CAST([ClinicalPictureID] AS VARCHAR(MAX)) AS [ClinicalPictureID],
		CAST([SpecimenID] AS VARCHAR(MAX)) AS [SpecimenID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vCodes_VirClinicalPictures) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    