
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Behandlingsdefinition. (bokning) ej inplementerad i TC""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TreatmentDefinitionID': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'TreatmentDefinitionID': "{'title_ui': None, 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': None, 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TreatmentDefinitionID AS VARCHAR(MAX)) AS TreatmentDefinitionID,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_TreatmentDefinitions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    