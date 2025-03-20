
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Besöks- och inskrivningsorsaker",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CVRCode': 'varchar(max)', 'IsSentToCVR': 'varchar(max)', 'Name': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TreatmentDefinition': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'VisitReasonID': 'varchar(max)'},
    column_descriptions={'VisitReasonID': "{'title_ui': 'Id', 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'IsSentToCVR': "{'title_ui': 'Till CVR', 'description': 'Sant om bokningar med besöksorsaken ska skickas till CVR - Centralt VäntetidsRegister'}", 'TreatmentDefinition': "{'title_ui': 'Behandlingsdefinition', 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CVRCode AS VARCHAR(MAX)) AS CVRCode,
		CAST(IsSentToCVR AS VARCHAR(MAX)) AS IsSentToCVR,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TreatmentDefinition AS VARCHAR(MAX)) AS TreatmentDefinition,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate,
		CAST(VisitReasonID AS VARCHAR(MAX)) AS VisitReasonID 
	FROM Intelligence.viewreader.vCodes_VisitReasons) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    