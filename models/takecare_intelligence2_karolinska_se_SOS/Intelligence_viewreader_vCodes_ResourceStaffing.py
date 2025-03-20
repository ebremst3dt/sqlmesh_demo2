
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="En resurs kan vara en befattning och olika personer kan bemanna denna befattning under olika perioder.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CareUnitID': 'varchar(max)', 'FromDate': 'varchar(max)', 'FromTime': 'varchar(max)', 'ProfessionResourceID': 'varchar(max)', 'StaffedByResourceID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ToDate': 'varchar(max)', 'ToTime': 'varchar(max)'},
    column_descriptions={'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'ProfessionResourceID': "{'title_ui': 'Befattning', 'description': None}", 'StaffedByResourceID': "{'title_ui': 'Bemannad av', 'description': None}", 'FromDate': "{'title_ui': 'Datum from', 'description': None}", 'FromTime': "{'title_ui': 'Tid from', 'description': None}", 'ToDate': "{'title_ui': 'Datum tom', 'description': None}", 'ToTime': "{'title_ui': 'Tid tom', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CONVERT(varchar(max), FromDate, 126) AS FromDate,
		CONVERT(varchar(max), FromTime, 126) AS FromTime,
		CAST(ProfessionResourceID AS VARCHAR(MAX)) AS ProfessionResourceID,
		CAST(StaffedByResourceID AS VARCHAR(MAX)) AS StaffedByResourceID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ToDate, 126) AS ToDate,
		CONVERT(varchar(max), ToTime, 126) AS ToTime 
	FROM Intelligence.viewreader.vCodes_ResourceStaffing) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    