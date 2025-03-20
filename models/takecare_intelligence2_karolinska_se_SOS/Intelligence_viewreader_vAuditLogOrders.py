
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Logg f�r best�llningar av f�rdjupad logguppf�ljning.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CreatedAtCareUnitID': 'varchar(max)', 'InvestigatedUserID': 'varchar(max)', 'InvestigationMinuteSpan': 'varchar(max)', 'InvestigationStartDate': 'varchar(max)', 'InvestigationStartTime': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderID': 'varchar(max)', 'OrderedByUserID': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'OrderID': "{'title_ui': 'Id', 'description': 'Order ID'}", 'InvestigationStartDate': "{'title_ui': 'Datum', 'description': 'Unders�ker detta datum'}", 'InvestigationStartTime': "{'title_ui': 'Start', 'description': 'Startpunkt f�r logguppf�ljningen'}", 'InvestigationMinuteSpan': "{'title_ui': None, 'description': 'L�ngd i minuter f�r logguppf�ljningen'}", 'PatientID': "{'title_ui': 'Patient pnr/rnr', 'description': 'Patientens person- eller reservnummer'}", 'InvestigatedUserID': "{'title_ui': 'Anv�ndare pnr/rnr', 'description': 'Anv�ndares person- eller reservnummer'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillh�r v�rdenhet', 'description': 'Den v�rdenhet d�r dokumentet �r skapat. Den v�rdenhet som beh�righet utg�r fr�n.'}", 'OrderedByUserID': "{'title_ui': None, 'description': 'Best�llares person- eller reservnummer'}", 'OrderDateTime': "{'title_ui': 'F�rdigst�llt', 'description': 'Datum d� ordern f�rdigst�lldes'}", 'TimestampRead': "{'title_ui': None, 'description': 'N�r data l�sts in fr�n TakeCare-databasen'}"},
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
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(InvestigatedUserID AS VARCHAR(MAX)) AS InvestigatedUserID,
		CAST(InvestigationMinuteSpan AS VARCHAR(MAX)) AS InvestigationMinuteSpan,
		CONVERT(varchar(max), InvestigationStartDate, 126) AS InvestigationStartDate,
		CONVERT(varchar(max), InvestigationStartTime, 126) AS InvestigationStartTime,
		CONVERT(varchar(max), OrderDateTime, 126) AS OrderDateTime,
		CAST(OrderID AS VARCHAR(MAX)) AS OrderID,
		CAST(OrderedByUserID AS VARCHAR(MAX)) AS OrderedByUserID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vAuditLogOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    