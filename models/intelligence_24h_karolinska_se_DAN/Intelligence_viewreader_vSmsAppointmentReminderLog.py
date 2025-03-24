
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Logg för bokningspåminnelser via SMS.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AppointmentID': 'varchar(max)', 'BatchDate': 'varchar(max)', 'BatchID': 'varchar(max)', 'BatchTime': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'EndDate': 'varchar(max)', 'LogDateTime': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReservationDate': 'varchar(max)', 'ReservationTime': 'varchar(max)', 'ResourceID': 'varchar(max)', 'Row': 'varchar(max)', 'SMSDestination': 'varchar(max)', 'SMSText': 'varchar(max)', 'SlotLength': 'varchar(max)', 'StartDate': 'varchar(max)', 'StatusID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'StartDate': "{'title_ui': None, 'description': 'Loggperiodens startdatum'}", 'EndDate': "{'title_ui': None, 'description': 'Loggperiodens slutdatum'}", 'BatchID': "{'title_ui': None, 'description': 'Internt id på körningen som genererar SMS'}", 'Row': "{'title_ui': None, 'description': 'Löpnummer på SMS i en körning'}", 'LogDateTime': "{'title_ui': None, 'description': 'Tiden då raden skrevs till loggen'}", 'BatchDate': "{'title_ui': None, 'description': 'Datum då körningen gjordes'}", 'BatchTime': "{'title_ui': None, 'description': 'Tid då körningen gjordes'}", 'PatientID': "{'title_ui': None, 'description': 'Person- eller reservnummer'}", 'AppointmentID': "{'title_ui': None, 'description': 'Bokningsnummer'}", 'ResourceID': "{'title_ui': None, 'description': 'Den resurs som bokningen gäller'}", 'ReservationDate': "{'title_ui': None, 'description': 'Bokningsdatum'}", 'ReservationTime': "{'title_ui': None, 'description': 'Bokningstid'}", 'StatusID': "{'title_ui': None, 'description': 'Bokningens status'}", 'CareUnitID': "{'title_ui': None, 'description': 'Vårdenhet'}", 'SlotLength': "{'title_ui': None, 'description': 'Längd i minuter på bokningen'}", 'SMSText': "{'title_ui': None, 'description': 'SMS-meddelande'}", 'SMSDestination': "{'title_ui': None, 'description': 'SMS-mottagarens telefonnummer'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AppointmentID] AS VARCHAR(MAX)) AS [AppointmentID],
		CONVERT(varchar(max), [BatchDate], 126) AS [BatchDate],
		CAST([BatchID] AS VARCHAR(MAX)) AS [BatchID],
		CONVERT(varchar(max), [BatchTime], 126) AS [BatchTime],
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CONVERT(varchar(max), [EndDate], 126) AS [EndDate],
		CONVERT(varchar(max), [LogDateTime], 126) AS [LogDateTime],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [ReservationDate], 126) AS [ReservationDate],
		CONVERT(varchar(max), [ReservationTime], 126) AS [ReservationTime],
		CAST([ResourceID] AS VARCHAR(MAX)) AS [ResourceID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SMSDestination] AS VARCHAR(MAX)) AS [SMSDestination],
		CAST([SMSText] AS VARCHAR(MAX)) AS [SMSText],
		CAST([SlotLength] AS VARCHAR(MAX)) AS [SlotLength],
		CONVERT(varchar(max), [StartDate], 126) AS [StartDate],
		CAST([StatusID] AS VARCHAR(MAX)) AS [StatusID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead] 
	FROM Intelligence.viewreader.vSmsAppointmentReminderLog) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    