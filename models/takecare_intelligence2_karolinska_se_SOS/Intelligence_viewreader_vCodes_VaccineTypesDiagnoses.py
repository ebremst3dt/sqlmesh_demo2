
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DiagnosisCode': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'VaccineTypeID': 'varchar(max)'},
    column_descriptions={'VaccineTypeID': "{'title_ui': 'Id', 'description': None}", 'DiagnosisCode': "{'title_ui': 'Diagnoskod', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DiagnosisCode AS VARCHAR(MAX)) AS DiagnosisCode,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(VaccineTypeID AS VARCHAR(MAX)) AS VaccineTypeID 
	FROM Intelligence.viewreader.vCodes_VaccineTypesDiagnoses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    