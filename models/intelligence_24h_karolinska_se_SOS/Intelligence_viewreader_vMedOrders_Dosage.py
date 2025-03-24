
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Datum, klockslag och eventuellt schema för framtida administrering. Om ej schemalagd kan många administreringar vid flera tidpunkter läggas upp. Data gäller från angivet datum tills vidare.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DosageID': 'varchar(max)', 'IsGivenOnFridays': 'varchar(max)', 'IsGivenOnMondays': 'varchar(max)', 'IsGivenOnSaturdays': 'varchar(max)', 'IsGivenOnSundays': 'varchar(max)', 'IsGivenOnThursdays': 'varchar(max)', 'IsGivenOnTuesdays': 'varchar(max)', 'IsGivenOnWednesdays': 'varchar(max)', 'PatientID': 'varchar(max)', 'Period': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'ScheduleType': 'varchar(max)', 'StartDate': 'varchar(max)', 'StartTime': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': 'Version', 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'DosageID': "{'title_ui': None, 'description': None}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tid då data sparats. Ibland förändras tiden mellan versioner, även om data inte förändrats.'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Användaren som sparat data'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Där data är sparat'}", 'StartDate': "{'title_ui': None, 'description': 'Ordinerat administrationsdatum. Gäller fr.o.m. detta datum.'}", 'StartTime': "{'title_ui': 'Ord.tid', 'description': 'Administrationstid, dvs. den tid på dygnet då administrering ska ske. Dosering kan vara utan tid.'}", 'ScheduleType': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'Period': '{\'title_ui\': \'Var ... :e dag/vecka/timme\', \'description\': \'"n" i schematyp\'}', 'IsGivenOnMondays': '{\'title_ui\': \'Måndag\', \'description\': \'Om dosering ska ske på måndagar (vid schematyp "var n:e vecka")\'}', 'IsGivenOnTuesdays': '{\'title_ui\': \'Tisdag\', \'description\': \'Om dosering ska ske på tisdagar (vid schematyp "var n:e vecka")\'}', 'IsGivenOnWednesdays': '{\'title_ui\': \'Onsdag\', \'description\': \'Om dosering ska ske på onsdagar (vid schematyp "var n:e vecka")\'}', 'IsGivenOnThursdays': '{\'title_ui\': \'Torsdag\', \'description\': \'Om dosering ska ske på torsdagar (vid schematyp "var n:e vecka")\'}', 'IsGivenOnFridays': '{\'title_ui\': \'Fredag\', \'description\': \'Om dosering ska ske på fredagar (vid schematyp "var n:e vecka")\'}', 'IsGivenOnSaturdays': '{\'title_ui\': \'Lördag\', \'description\': \'Om dosering ska ske på lördagar (vid schematyp "var n:e vecka")\'}', 'IsGivenOnSundays': '{\'title_ui\': \'Söndag\', \'description\': \'Om dosering ska ske på söndagar (vid schematyp "var n:e vecka")\'}', 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(DosageID AS VARCHAR(MAX)) AS DosageID,
		CAST(IsGivenOnFridays AS VARCHAR(MAX)) AS IsGivenOnFridays,
		CAST(IsGivenOnMondays AS VARCHAR(MAX)) AS IsGivenOnMondays,
		CAST(IsGivenOnSaturdays AS VARCHAR(MAX)) AS IsGivenOnSaturdays,
		CAST(IsGivenOnSundays AS VARCHAR(MAX)) AS IsGivenOnSundays,
		CAST(IsGivenOnThursdays AS VARCHAR(MAX)) AS IsGivenOnThursdays,
		CAST(IsGivenOnTuesdays AS VARCHAR(MAX)) AS IsGivenOnTuesdays,
		CAST(IsGivenOnWednesdays AS VARCHAR(MAX)) AS IsGivenOnWednesdays,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Period AS VARCHAR(MAX)) AS Period,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(ScheduleType AS VARCHAR(MAX)) AS ScheduleType,
		CONVERT(varchar(max), StartDate, 126) AS StartDate,
		CONVERT(varchar(max), StartTime, 126) AS StartTime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMedOrders_Dosage) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    