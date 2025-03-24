
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Data från register över externa vårdenheter. Användaren definierar, per installation, versionsrader för en vårdenhet, samt vilka kolumner som registret använder. Enhet som tagits bort ur textfilen som laddar registret kommer ej med i tabellen.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CostDepartment': 'varchar(max)', 'EXID': 'varchar(max)', 'ParentEXID': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UnitLevelID': 'varchar(max)', 'UnitName': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'WorkPlaceCode': 'varchar(max)'},
    column_descriptions={'EXID': "{'title_ui': None, 'description': 'Internt id för enheten'}", 'Row': "{'title_ui': None, 'description': 'Användardefinierad version av enheten. Högsta versionsnummer som innefattar ett givet datum är giltig.'}", 'ValidFromDate': "{'title_ui': None, 'description': 'Avgör om versionen är eller har varit giltig för ett givet datum. Se ValidThroughDate och Row.'}", 'ValidThroughDate': "{'title_ui': None, 'description': 'Avgör om versionen är eller har varit giltig för ett givet datum. Se ValidFromDate och Row.'}", 'UnitLevelID': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'ParentEXID': "{'title_ui': None, 'description': 'EXID för enhet som denna enhet sorterar under. Se kolumn UnitLevelID'}", 'UnitName': "{'title_ui': None, 'description': 'Enhetens namn'}", 'CostDepartment': "{'title_ui': None, 'description': 'Kostnadsställe. Ej att förväxlas med Id-kolumnen Codes_ExternalUnitIDs.CostCenter.'}", 'WorkPlaceCode': "{'title_ui': None, 'description': 'Arbetsplatskod. Matas enl. uppg. in utan inledande länskod'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CostDepartment] AS VARCHAR(MAX)) AS [CostDepartment],
		CAST([EXID] AS VARCHAR(MAX)) AS [EXID],
		CAST([ParentEXID] AS VARCHAR(MAX)) AS [ParentEXID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([UnitLevelID] AS VARCHAR(MAX)) AS [UnitLevelID],
		CAST([UnitName] AS VARCHAR(MAX)) AS [UnitName],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate],
		CAST([WorkPlaceCode] AS VARCHAR(MAX)) AS [WorkPlaceCode] 
	FROM Intelligence.viewreader.vCodes_ExternalUnits) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    