
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Denna tabell innehåller huvudsakligen GVR-transaktioners metadata som används internt i TakeCare.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AddedDatetime': 'varchar(max)', 'FileName': 'varchar(max)', 'InternalGVRServiceTypeID': 'varchar(max)', 'IsCompleted': 'varchar(max)', 'LatestReplyDatetime': 'varchar(max)', 'LatestSendDatetime': 'varchar(max)', 'NumberOfTries': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReceivedBytes': 'varchar(max)', 'ReplyStatusCode': 'varchar(max)', 'ReplyStatusMessage': 'varchar(max)', 'SNODApplicationErrorID': 'varchar(max)', 'SNODCommunicationErrorID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionGroup': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'PatientID': "{'title_ui': None, 'description': 'Patientens Person-/reservnummer'}", 'IsCompleted': "{'title_ui': None, 'description': 'Behandlad/ej färdigbehandlad transaktion. D.v.s. True betyder att TakeCare accepterar svaret eller har gett upp försöken att sända transaktionen - inga fler försök kommer att göras.'}", 'InternalGVRServiceTypeID': "{'title_ui': None, 'description': 'Internt hårdkodat ID för den GVR-tjänst som använts'}", 'AddedDatetime': "{'title_ui': None, 'description': 'Tidsstämpel då post lades i kön/loggfilen'}", 'LatestSendDatetime': "{'title_ui': None, 'description': 'Tidsstämpel då post senast skickades till GVR-servern'}", 'LatestReplyDatetime': "{'title_ui': None, 'description': 'Tidsstämpel för senaste svaret från GVR-servern'}", 'SNODCommunicationErrorID': "{'title_ui': None, 'description': {'break': None}}", 'SNODApplicationErrorID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'ReceivedBytes': "{'title_ui': None, 'description': 'Antal bytes data i svaret från GVR'}", 'NumberOfTries': "{'title_ui': None, 'description': 'Antal försök att sända posten'}", 'TransactionGroup': "{'title_ui': None, 'description': 'Om transaktionen är en öppenvårds-, slutenvårds- eller vårdperiodstransaktion'}", 'ReplyStatusCode': "{'title_ui': None, 'description': {'break': None}}", 'ReplyStatusMessage': "{'title_ui': None, 'description': 'Meddelandet som hör ihop med statuskod, från GVR-tjänsten, om svar finns'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), AddedDatetime, 126) AS AddedDatetime,
		CAST(FileName AS VARCHAR(MAX)) AS FileName,
		CAST(InternalGVRServiceTypeID AS VARCHAR(MAX)) AS InternalGVRServiceTypeID,
		CAST(IsCompleted AS VARCHAR(MAX)) AS IsCompleted,
		CONVERT(varchar(max), LatestReplyDatetime, 126) AS LatestReplyDatetime,
		CONVERT(varchar(max), LatestSendDatetime, 126) AS LatestSendDatetime,
		CAST(NumberOfTries AS VARCHAR(MAX)) AS NumberOfTries,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReceivedBytes AS VARCHAR(MAX)) AS ReceivedBytes,
		CAST(ReplyStatusCode AS VARCHAR(MAX)) AS ReplyStatusCode,
		CAST(ReplyStatusMessage AS VARCHAR(MAX)) AS ReplyStatusMessage,
		CAST(SNODApplicationErrorID AS VARCHAR(MAX)) AS SNODApplicationErrorID,
		CAST(SNODCommunicationErrorID AS VARCHAR(MAX)) AS SNODCommunicationErrorID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TransactionGroup AS VARCHAR(MAX)) AS TransactionGroup,
		CAST(TransactionID AS VARCHAR(MAX)) AS TransactionID 
	FROM Intelligence.viewreader.vGVR) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    