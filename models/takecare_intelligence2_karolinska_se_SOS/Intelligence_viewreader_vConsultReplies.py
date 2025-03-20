
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Konsultationssvar. Är svar på beställning från remittent. Bedömarens svar skickas till svarsmottagaren. Svar kan både skickas iväg och tas emot. Remisser kan skickas mellan olika system och då kan interna id:n som vårdenhet saknas.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AlphanumericalLID': 'varchar(max)', 'ConsultRefDocumentID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'IsElectronic': 'varchar(max)', 'IsElectronicFromExternalSystem': 'varchar(max)', 'LID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferralReplyComment': 'varchar(max)', 'ReplyDate': 'varchar(max)', 'ReplyRecipientCareUnitAddress': 'varchar(max)', 'ReplyRecipientCareUnitEAN': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'ReplyRecipientSID': 'varchar(max)', 'ReplyText': 'varchar(max)', 'ReplyTime': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ReplyingCareUnitAddress': 'varchar(max)', 'ReplyingCareUnitExternalID': 'varchar(max)', 'ReplyingCareUnitID': 'varchar(max)', 'ReplyingCareUnitKombika': 'varchar(max)', 'ReplyingCareUnitTelephone': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignerName': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TimestampSigned': 'varchar(max)', 'TrackingStatusID': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'CreatedByUserID': "{'title_ui': 'Skapad av', 'description': None}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': None}", 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': 'Kallas även handläggare'}", 'SignerName': "{'title_ui': 'Signeringsansvarig', 'description': 'Namn om personnummer saknas, ex. om svar kommit från annat system'}", 'TimestampSigned': "{'title_ui': 'Signerad', 'description': None}", 'ReplyingCareUnitID': "{'title_ui': 'Svarande enhet', 'description': 'Enheten som tar emot remissen och skickar svaret. Alla på denna vårdenhet får se denna remiss. Kan vara NULL om ej elektronisk remiss.'}", 'ReplyingCareUnitKombika': "{'title_ui': 'Svarande enhet', 'description': 'Svarande enhets kombika/EXID'}", 'ReplyingCareUnitAddress': "{'title_ui': 'Svarande enhet', 'description': 'Svarande enhets adress'}", 'ReplyingCareUnitTelephone': "{'title_ui': 'Svarande enhet', 'description': 'Svarande enhets telefonnummer'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'Remitterande enhet', 'description': 'Mottagare av remissvar. Styr behörighet, men kan vara NULL om ej elektronisk remiss.'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets kombika/EXID'}", 'ReplyRecipientCareUnitAddress': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets adress'}", 'ReplyRecipientCareUnitEAN': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande enhets EAN-kod'}", 'ReplyRecipientSID': "{'title_ui': 'Remitterande enhet', 'description': 'Remitterande system'}", 'ConsultRefDocumentID': "{'title_ui': None, 'description': 'Dokument-id för den beställning detta är ett svar på. Används för att koppla ihop svar med beställning.'}", 'TrackingStatusID': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'IsElectronic': "{'title_ui': None, 'description': 'Om svaret inkommit på elektronisk väg'}", 'LID': "{'title_ui': 'Svarsidentitet', 'description': 'ID för svaret'}", 'AlphanumericalLID': "{'title_ui': 'Svarsidentitet', 'description': 'Alfanumerisk LID då detta förekommer'}", 'ReplyDate': "{'title_ui': 'Svarsdatum', 'description': 'Händelsetid. Matas in manuellt i svarsbilden eller läses in från externt svar.'}", 'ReplyTime': "{'title_ui': 'Svarstid', 'description': None}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Sätts när svaret skickas eller tas emot'}", 'ReplyText': "{'title_ui': 'Svarstext', 'description': 'Remissutlåtande i fritext'}", 'ExternalUnitIdTypeCode': "{'title_ui': None, 'description': 'Id-typ för beställningen'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'Remitterande enhet', 'description': 'Kod för extern enhet för svarsmottagare'}", 'ReplyingCareUnitExternalID': "{'title_ui': 'Svarande enhet', 'description': 'Kod för extern enhet för mottagare'}", 'ReferralReplyComment': "{'title_ui': 'Remittentens kommentar', 'description': 'Remittentens kommentar till svaret. Lagras om systeminställning är satt'}", 'IsElectronicFromExternalSystem': "{'title_ui': None, 'description': 'Om svaret skapats av inläsare för externa elektroniska remisser och svar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AlphanumericalLID AS VARCHAR(MAX)) AS AlphanumericalLID,
		CAST(ConsultRefDocumentID AS VARCHAR(MAX)) AS ConsultRefDocumentID,
		CAST(CreatedByUserID AS VARCHAR(MAX)) AS CreatedByUserID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(ExternalUnitIdTypeCode AS VARCHAR(MAX)) AS ExternalUnitIdTypeCode,
		CAST(IsElectronic AS VARCHAR(MAX)) AS IsElectronic,
		CAST(IsElectronicFromExternalSystem AS VARCHAR(MAX)) AS IsElectronicFromExternalSystem,
		CAST(LID AS VARCHAR(MAX)) AS LID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReferralReplyComment AS VARCHAR(MAX)) AS ReferralReplyComment,
		CONVERT(varchar(max), ReplyDate, 126) AS ReplyDate,
		CAST(ReplyRecipientCareUnitAddress AS VARCHAR(MAX)) AS ReplyRecipientCareUnitAddress,
		CAST(ReplyRecipientCareUnitEAN AS VARCHAR(MAX)) AS ReplyRecipientCareUnitEAN,
		CAST(ReplyRecipientCareUnitExternalID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitExternalID,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CAST(ReplyRecipientSID AS VARCHAR(MAX)) AS ReplyRecipientSID,
		CAST(ReplyText AS VARCHAR(MAX)) AS ReplyText,
		CONVERT(varchar(max), ReplyTime, 126) AS ReplyTime,
		CONVERT(varchar(max), ReplyTimestamp, 126) AS ReplyTimestamp,
		CAST(ReplyingCareUnitAddress AS VARCHAR(MAX)) AS ReplyingCareUnitAddress,
		CAST(ReplyingCareUnitExternalID AS VARCHAR(MAX)) AS ReplyingCareUnitExternalID,
		CAST(ReplyingCareUnitID AS VARCHAR(MAX)) AS ReplyingCareUnitID,
		CAST(ReplyingCareUnitKombika AS VARCHAR(MAX)) AS ReplyingCareUnitKombika,
		CAST(ReplyingCareUnitTelephone AS VARCHAR(MAX)) AS ReplyingCareUnitTelephone,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CAST(SignerName AS VARCHAR(MAX)) AS SignerName,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CONVERT(varchar(max), TimestampSigned, 126) AS TimestampSigned,
		CAST(TrackingStatusID AS VARCHAR(MAX)) AS TrackingStatusID,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vConsultReplies) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    