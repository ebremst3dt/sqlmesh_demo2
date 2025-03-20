
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Registrering/korrigering av tillgänglighetsuppgifter för öppenvården.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AppointmentCareGuaranteeCode': 'varchar(max)', 'AppointmentOrigin': 'varchar(max)', 'AppointmentReferralDateTime': 'varchar(max)', 'AppointmentReservationDateTime': 'varchar(max)', 'AppointmentTaskDecisionDateTime': 'varchar(max)', 'AppointmentTimestampCreated': 'varchar(max)', 'FileName': 'varchar(max)', 'GVRPostponementReasonCode': 'varchar(max)', 'ReferralTypeCode': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'AppointmentCareGuaranteeCode': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'AppointmentOrigin': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'AppointmentTimestampCreated': "{'title_ui': None, 'description': 'Datum då bokningen skapats i systemet'}", 'AppointmentReservationDateTime': "{'title_ui': None, 'description': 'Datum för när besöket ska ske'}", 'AppointmentReferralDateTime': "{'title_ui': None, 'description': 'Datum då remissen skapades'}", 'ReferralTypeCode': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'AppointmentTaskDecisionDateTime': "{'title_ui': None, 'description': 'Startdatum för åtgärd'}", 'GVRPostponementReasonCode': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AppointmentCareGuaranteeCode AS VARCHAR(MAX)) AS AppointmentCareGuaranteeCode,
		CAST(AppointmentOrigin AS VARCHAR(MAX)) AS AppointmentOrigin,
		CONVERT(varchar(max), AppointmentReferralDateTime, 126) AS AppointmentReferralDateTime,
		CONVERT(varchar(max), AppointmentReservationDateTime, 126) AS AppointmentReservationDateTime,
		CONVERT(varchar(max), AppointmentTaskDecisionDateTime, 126) AS AppointmentTaskDecisionDateTime,
		CONVERT(varchar(max), AppointmentTimestampCreated, 126) AS AppointmentTimestampCreated,
		CAST(FileName AS VARCHAR(MAX)) AS FileName,
		CAST(GVRPostponementReasonCode AS VARCHAR(MAX)) AS GVRPostponementReasonCode,
		CAST(ReferralTypeCode AS VARCHAR(MAX)) AS ReferralTypeCode,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TransactionID AS VARCHAR(MAX)) AS TransactionID 
	FROM Intelligence.viewreader.vGVR_OutpatientAvailability) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    