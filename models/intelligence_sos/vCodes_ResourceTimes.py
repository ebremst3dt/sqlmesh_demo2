
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read

    
@model(
    description="Definierade tider för en resurs, en tid kan t.ex vara 'Rond'",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BookableDaysAhead': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'FromDate': 'varchar(max)', 'FromTime': 'varchar(max)', 'ResourceID': 'varchar(max)', 'SlotLength': 'varchar(max)', 'TimeTypeID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ToDate': 'varchar(max)', 'ToTime': 'varchar(max)', 'WeekdayID': 'varchar(max)'},
    column_descriptions={'ResourceID': "{'title_ui': 'Resurs', 'description': None}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'FromDate': "{'title_ui': 'Datum from', 'description': 'From detta datum gäller tiderna som definieras av denna rad.'}", 'FromTime': "{'title_ui': 'Tid from', 'description': 'Klockslag när tiderna börjar'}", 'ToDate': '{\'title_ui\': \'Datum tom\', \'description\': \'From detta datum gäller tiderna som definieras av denna rad. Numera matchar alltid denna kolumn "From" men äldre data kan ha tider definierade över ett spann med dagar.\'}', 'ToTime': "{'title_ui': 'Tid tom', 'description': 'Klockslag när tiderna slutar'}", 'TimeTypeID': "{'title_ui': 'Tidstyp', 'description': 'Vilken typ av aktivitet som ska ske denna tid, t.ex mottagning eller rond.'}", 'WeekdayID': "{'title_ui': 'Dag', 'description': {'break': [None, None]}}", 'SlotLength': "{'title_ui': 'Längd', 'description': 'Hur lång varje tid är som läggs in mellan de specificerade klockslagen (i minuter). T.ex kan man ha mottagning mellan 08:00 och 11:00 och varje mottagningstid är 20 min.'}", 'BookableDaysAhead': "{'title_ui': 'Bokningsbar dagar före', 'description': 'Här kan man specificera om det bara ska gå att boka tiden max ett visst antal dagar i förväg. Vissa användare i TakeCare har rätt att boka ändå, så bokningar kan finnas som inte uppfyller denna regel.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
    enabled=False
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
		CAST([BookableDaysAhead] AS VARCHAR(MAX)) AS [BookableDaysAhead],
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CONVERT(varchar(max), [FromDate], 126) AS [FromDate],
		CAST([FromTime] AS VARCHAR(MAX)) AS [FromTime],
		CAST([ResourceID] AS VARCHAR(MAX)) AS [ResourceID],
		CAST([SlotLength] AS VARCHAR(MAX)) AS [SlotLength],
		CAST([TimeTypeID] AS VARCHAR(MAX)) AS [TimeTypeID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [ToDate], 126) AS [ToDate],
		CAST([ToTime] AS VARCHAR(MAX)) AS [ToTime],
		CAST([WeekdayID] AS VARCHAR(MAX)) AS [WeekdayID] 
	FROM Intelligence.viewreader.vCodes_ResourceTimes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    