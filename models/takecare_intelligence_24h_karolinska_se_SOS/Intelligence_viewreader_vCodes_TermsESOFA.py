
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ESOFATypeID': 'varchar(max)', 'ExcludeValuesFromDays': 'varchar(max)', 'IncludeLabValuesFromHours': 'varchar(max)', 'IncludeValuesFromDays': 'varchar(max)', 'IncludeValuesFromHours': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={},
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
		CAST(ESOFATypeID AS VARCHAR(MAX)) AS ESOFATypeID,
		CAST(ExcludeValuesFromDays AS VARCHAR(MAX)) AS ExcludeValuesFromDays,
		CAST(IncludeLabValuesFromHours AS VARCHAR(MAX)) AS IncludeLabValuesFromHours,
		CAST(IncludeValuesFromDays AS VARCHAR(MAX)) AS IncludeValuesFromDays,
		CAST(IncludeValuesFromHours AS VARCHAR(MAX)) AS IncludeValuesFromHours,
		CAST(TermID AS VARCHAR(MAX)) AS TermID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_TermsESOFA) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    