
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""None""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DaysFromStart': 'varchar(max)', 'Dose': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'VaccinationScheduleID': 'varchar(max)', 'VaccineTypeID': 'varchar(max)'},
    column_descriptions={'VaccinationScheduleID': "{'title_ui': 'Id', 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'Dose': "{'title_ui': 'Dos nr', 'description': 'Ordningsnummer för dostillfälle'}", 'VaccineTypeID': "{'title_ui': 'Vaccintyp-Id', 'description': 'Id för vaccintyp'}", 'DaysFromStart': "{'title_ui': 'Dagar från start', 'description': 'Antal dagar från start av schemat'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(DaysFromStart AS VARCHAR(MAX)) AS DaysFromStart,
		CAST(Dose AS VARCHAR(MAX)) AS Dose,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(VaccinationScheduleID AS VARCHAR(MAX)) AS VaccinationScheduleID,
		CAST(VaccineTypeID AS VARCHAR(MAX)) AS VaccineTypeID 
	FROM Intelligence.viewreader.vCodes_VaccinationSchedulesPosts) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    