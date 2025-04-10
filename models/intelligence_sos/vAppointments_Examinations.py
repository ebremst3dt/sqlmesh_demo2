
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Undersökningar kopplade till ett bokningsunderlag. Använd tidpunkt sparad för att se vilken version av bokningsunderlaget som hade vilka kopplingar.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AppointmentRow': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'ExaminationDate': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'ExaminationTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'AppointmentRow': "{'title_ui': None, 'description': 'Internt löpnummer'}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Del av främmande nyckel till undersökning'}", 'ExaminationID': "{'title_ui': 'Undersökning/förberedelse', 'description': None}", 'ExaminationDate': "{'title_ui': 'Datum/tid', 'description': 'Datum när undersökningen planeras att genomföras.'}", 'ExaminationTime': "{'title_ui': 'Datum/tid', 'description': 'Tid när undersökningen planeras att genomföras.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['AppointmentRow', 'CareUnitID', 'ExaminationID', 'PatientID']
    ),
    cron="@daily",
    start=start,
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
		CAST([AppointmentRow] AS VARCHAR(MAX)) AS [AppointmentRow],
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CONVERT(varchar(max), [ExaminationDate], 126) AS [ExaminationDate],
		CAST([ExaminationID] AS VARCHAR(MAX)) AS [ExaminationID],
		CONVERT(varchar(max), [ExaminationTime], 126) AS [ExaminationTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vAppointments_Examinations) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    