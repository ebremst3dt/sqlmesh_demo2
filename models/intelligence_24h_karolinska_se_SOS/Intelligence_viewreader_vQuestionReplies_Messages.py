
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="I denna tabell lagras fråga (alltid första raden) samt ev. svar (alltid andra raden). För de kolumner vars namn börjar på SenderReceiver gäller att om TypeID är 1 eller 3 avses avsändare, och om TypeID är 2 eller 4 avses mottagare.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ContactInfo': 'varchar(max)', 'DocumentID': 'varchar(max)', 'MessageHeading': 'varchar(max)', 'MessageText': 'varchar(max)', 'MessageTimestampSaved': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SenderReceiverCareUnitID': 'varchar(max)', 'SenderReceiverUser': 'varchar(max)', 'SenderReceiverUserHSAID': 'varchar(max)', 'SenderReceiverUserID': 'varchar(max)', 'SenderReceiverUserName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSent': 'varchar(max)', 'TimestampSigned': 'varchar(max)', 'TypeID': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': 'Pnr', 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Unik rad för meddelande'}", 'TypeID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'MessageTimestampSaved': "{'title_ui': None, 'description': 'Tidpunkt då meddelandet sparades i journalen'}", 'TimestampSigned': "{'title_ui': None, 'description': 'Tidpunkt då meddelandet signerades'}", 'TimestampSent': "{'title_ui': None, 'description': 'Tidpunkt då meddelandet ursprungligen skickades'}", 'SenderReceiverUserID': "{'title_ui': None, 'description': 'Användar-id för avsändare/mottagare'}", 'SenderReceiverUserName': "{'title_ui': None, 'description': 'Användarnamn för avsändare/mottagare'}", 'SenderReceiverUser': "{'title_ui': None, 'description': 'Namn på avsändare/mottagare'}", 'SenderReceiverUserHSAID': "{'title_ui': None, 'description': 'HSA-id för avsändare/mottagare'}", 'SenderReceiverCareUnitID': "{'title_ui': None, 'description': 'Vårdenhet som meddelandet är skickat från alt. adresserat till'}", 'MessageHeading': "{'title_ui': None, 'description': 'Rubriken på meddelandet'}", 'ContactInfo': "{'title_ui': None, 'description': 'Kontaktinfo för extern avsändare (endast relevant om TypeID är 2 eller 4)'}", 'MessageText': "{'title_ui': None, 'description': 'Meddelandetext'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(ContactInfo AS VARCHAR(MAX)) AS ContactInfo,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(MessageHeading AS VARCHAR(MAX)) AS MessageHeading,
		CAST(MessageText AS VARCHAR(MAX)) AS MessageText,
		CONVERT(varchar(max), MessageTimestampSaved, 126) AS MessageTimestampSaved,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SenderReceiverCareUnitID AS VARCHAR(MAX)) AS SenderReceiverCareUnitID,
		CAST(SenderReceiverUser AS VARCHAR(MAX)) AS SenderReceiverUser,
		CAST(SenderReceiverUserHSAID AS VARCHAR(MAX)) AS SenderReceiverUserHSAID,
		CAST(SenderReceiverUserID AS VARCHAR(MAX)) AS SenderReceiverUserID,
		CAST(SenderReceiverUserName AS VARCHAR(MAX)) AS SenderReceiverUserName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSent, 126) AS TimestampSent,
		CONVERT(varchar(max), TimestampSigned, 126) AS TimestampSigned,
		CAST(TypeID AS VARCHAR(MAX)) AS TypeID,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vQuestionReplies_Messages) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    