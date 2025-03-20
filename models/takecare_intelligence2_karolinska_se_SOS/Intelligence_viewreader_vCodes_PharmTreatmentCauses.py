
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Behandlingsorsaker för farmlab-analyser.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'TimestampRead': 'varchar(max)', 'TreatmentCauseCode': 'varchar(max)', 'TreatmentCauseText': 'varchar(max)'},
    column_descriptions={'TreatmentCauseCode': "{'title_ui': 'Orsak till behandling', 'description': 'Orsak till behandling, kod'}", 'TreatmentCauseText': "{'title_ui': 'Orsak till behandling', 'description': 'Orsak till behandling, text'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TreatmentCauseCode AS VARCHAR(MAX)) AS TreatmentCauseCode,
		CAST(TreatmentCauseText AS VARCHAR(MAX)) AS TreatmentCauseText 
	FROM Intelligence.viewreader.vCodes_PharmTreatmentCauses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    