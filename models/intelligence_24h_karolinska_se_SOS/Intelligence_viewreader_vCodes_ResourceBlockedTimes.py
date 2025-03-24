
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Tider som är spärrade, dvs ingen bokning eller tidsregistrering  kan ske på dessa tider utom av användare med särskild behörighet.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'FromDatetime': 'varchar(max)', 'ResourceID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ToDatetime': 'varchar(max)'},
    column_descriptions={'ResourceID': "{'title_ui': 'Resurs', 'description': None}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'FromDatetime': "{'title_ui': 'Tidpunkt from', 'description': 'From detta datum och klockslag är alla tider för resursen spärrade'}", 'ToDatetime': "{'title_ui': 'Tidpunkt tom', 'description': 'Tom detta datum och klockslag är alla tider för resursen spärrade'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CONVERT(varchar(max), FromDatetime, 126) AS FromDatetime,
		CAST(ResourceID AS VARCHAR(MAX)) AS ResourceID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ToDatetime, 126) AS ToDatetime 
	FROM Intelligence.viewreader.vCodes_ResourceBlockedTimes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    