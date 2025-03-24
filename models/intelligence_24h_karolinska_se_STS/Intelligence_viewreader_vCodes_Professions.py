
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Yrkesgrupper",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'HSATitle': 'varchar(max)', 'NamePlural': 'varchar(max)', 'NameShort': 'varchar(max)', 'NameSingular': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'SLLID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'ProfessionID': "{'title_ui': 'Id', 'description': None}", 'NameSingular': "{'title_ui': 'Namn singular', 'description': None}", 'NamePlural': "{'title_ui': 'Namn plural', 'description': None}", 'NameShort': "{'title_ui': 'Namn kort', 'description': None}", 'SLLID': "{'title_ui': 'Kod', 'description': 'Extern kod för yrkesgruppen'}", 'HSATitle': "{'title_ui': 'hsaTitel-kod', 'description': 'legitimerad yrkesgrupp enligt SFS 1998:531, koderna kommer ifrån Socialstyrelsen'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum då data är giltigt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
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
		CAST([HSATitle] AS VARCHAR(MAX)) AS [HSATitle],
		CAST([NamePlural] AS VARCHAR(MAX)) AS [NamePlural],
		CAST([NameShort] AS VARCHAR(MAX)) AS [NameShort],
		CAST([NameSingular] AS VARCHAR(MAX)) AS [NameSingular],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([SLLID] AS VARCHAR(MAX)) AS [SLLID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate] 
	FROM Intelligence.viewreader.vCodes_Professions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    