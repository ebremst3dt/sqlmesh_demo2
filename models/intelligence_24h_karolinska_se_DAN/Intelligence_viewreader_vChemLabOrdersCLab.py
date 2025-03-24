
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Beställning Kemlabb (CLab). Nedlagd, användes till 2004.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'Diagnosis': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Invoicee': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'LID': 'varchar(max)', 'Message': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'ReplyRecipient': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'UrineSamplingEndDate': 'varchar(max)', 'UrineSamplingEndTime': 'varchar(max)', 'UrineSamplingStartDate': 'varchar(max)', 'UrineSamplingStartTime': 'varchar(max)', 'UrineSamplingVolume': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'ReferringDoctorUserID': "{'title_ui': None, 'description': 'Pnr för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctorUserName': "{'title_ui': None, 'description': 'Remitterande läkare/vidimeringsansvarig användarnamn'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Remitterande läkare/vidimeringsansvarig klartext'}", 'LID': "{'title_ui': 'Lid', 'description': 'Labbeställning nr'}", 'SamplingDate': "{'title_ui': 'Provtagn. tid', 'description': 'Datum för provtagning'}", 'SamplingTime': "{'title_ui': 'Provtagn. tid', 'description': 'Tid för provtagning'}", 'IsEmergency': "{'title_ui': 'Akut', 'description': None}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'UrineSamplingStartDate': "{'title_ui': 'Urinsamling från datum', 'description': None}", 'UrineSamplingStartTime': "{'title_ui': 'Urinsamling från kl', 'description': None}", 'UrineSamplingEndDate': "{'title_ui': 'Urinsamling till datum', 'description': None}", 'UrineSamplingEndTime': "{'title_ui': 'Urinsamling till kl', 'description': None}", 'UrineSamplingVolume': "{'title_ui': 'Urinsamling volym', 'description': None}", 'Diagnosis': "{'title_ui': 'Diagnos/Frågeställning', 'description': None}", 'Message': "{'title_ui': 'Meddelande till lab', 'description': None}", 'ReplyRecipient': "{'title_ui': 'Svarsmottagare', 'description': None}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'Invoicee': "{'title_ui': 'Fakturamottagare', 'description': None}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([Diagnosis] AS VARCHAR(MAX)) AS [Diagnosis],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Invoicee] AS VARCHAR(MAX)) AS [Invoicee],
		CAST([InvoiceeCareUnitKombika] AS VARCHAR(MAX)) AS [InvoiceeCareUnitKombika],
		CAST([IsBloodInfection] AS VARCHAR(MAX)) AS [IsBloodInfection],
		CAST([IsEmergency] AS VARCHAR(MAX)) AS [IsEmergency],
		CAST([LID] AS VARCHAR(MAX)) AS [LID],
		CAST([Message] AS VARCHAR(MAX)) AS [Message],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ReferringDoctor] AS VARCHAR(MAX)) AS [ReferringDoctor],
		CAST([ReferringDoctorUserID] AS VARCHAR(MAX)) AS [ReferringDoctorUserID],
		CAST([ReferringDoctorUserName] AS VARCHAR(MAX)) AS [ReferringDoctorUserName],
		CAST([ReplyRecipient] AS VARCHAR(MAX)) AS [ReplyRecipient],
		CAST([ReplyRecipientCareUnitKombika] AS VARCHAR(MAX)) AS [ReplyRecipientCareUnitKombika],
		CONVERT(varchar(max), [SamplingDate], 126) AS [SamplingDate],
		CONVERT(varchar(max), [SamplingTime], 126) AS [SamplingTime],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [UrineSamplingEndDate], 126) AS [UrineSamplingEndDate],
		CONVERT(varchar(max), [UrineSamplingEndTime], 126) AS [UrineSamplingEndTime],
		CONVERT(varchar(max), [UrineSamplingStartDate], 126) AS [UrineSamplingStartDate],
		CONVERT(varchar(max), [UrineSamplingStartTime], 126) AS [UrineSamplingStartTime],
		CAST([UrineSamplingVolume] AS VARCHAR(MAX)) AS [UrineSamplingVolume],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vChemLabOrdersCLab) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_DAN")
    