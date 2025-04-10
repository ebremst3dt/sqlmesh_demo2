
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Vårdplanering används för att hantera planering och bevakning av aktiviteter inför en inskrivning eller operation.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ActionGroupID': 'varchar(max)', 'AppointmentID': 'varchar(max)', 'CarePlanComment': 'varchar(max)', 'CarePlanTypeID': 'varchar(max)', 'Comment': 'varchar(max)', 'ConsultationDate': 'varchar(max)', 'ConsultationDocumentID': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DateCreated': 'varchar(max)', 'DocumentID': 'varchar(max)', 'InterpreterLanguage': 'varchar(max)', 'IsAvailableOnShortNotice': 'varchar(max)', 'IsNeedingInterpreter': 'varchar(max)', 'IsSpecialistReferral': 'varchar(max)', 'ListStatusID': 'varchar(max)', 'MunicipalityCompositeID': 'varchar(max)', 'NotificationDatetime': 'varchar(max)', 'NotificationTypeID': 'varchar(max)', 'NotifiedByUserID': 'varchar(max)', 'OrdererUnitID': 'varchar(max)', 'OrdererUnitName': 'varchar(max)', 'PatientID': 'varchar(max)', 'PostponementReasonID': 'varchar(max)', 'PriorityID': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'RequestedDate': 'varchar(max)', 'ResponsibleUserID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SectionKombika': 'varchar(max)', 'SerialNumber': 'varchar(max)', 'SpecialistRefValidThroughDate': 'varchar(max)', 'StartDate': 'varchar(max)', 'Study': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)', 'VisitReasonID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': 'Vårdplansid', 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Senast ändrad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'CreatedAtCareUnitID': "{'title_ui': 'Skapad på', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'Användaren som skapade den första versionen av dokumentet'}", 'DateCreated': "{'title_ui': 'Skapad', 'description': 'Datum då vårdplan skapades'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Visas i listan över vårdplaneringar, inte samma som fältet CarePlanComment. Oftast typ av vårdplan och vårdkontaktsorsak.'}", 'RegistrationStatusID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'ListStatusID': "{'title_ui': 'Status', 'description': 'Den lista där vårdplaneringen visas'}", 'RequestedDate': "{'title_ui': 'Önskat datum', 'description': 'Önskat datum för första vårdkontakt'}", 'StartDate': "{'title_ui': 'Start', 'description': 'Datum då vårdplanen påbörjades. Utgör underlag för beräkning av vårdtid.'}", 'ResponsibleUserID': "{'title_ui': 'Ansvarig', 'description': 'Den som är ansvarig för vårdplaneringen'}", 'NotificationTypeID': "{'title_ui': 'Kallelsesätt', 'description': None}", 'NotificationDatetime': "{'title_ui': 'Kallad', 'description': None}", 'NotifiedByUserID': "{'title_ui': 'Kallad', 'description': None}", 'CarePlanTypeID': "{'title_ui': 'Typ av vårdplan', 'description': None}", 'VisitReasonID': "{'title_ui': 'Vårdkontaktsorsak', 'description': None}", 'PriorityID': "{'title_ui': 'Prioritet', 'description': None}", 'SectionKombika': "{'title_ui': 'Ekonomisk enhet', 'description': 'En av vårdenhetens ekonomiska enheter (Kombika/EXID)'}", 'PostponementReasonID': "{'title_ui': 'Framflyttad vård', 'description': 'Orsaken till varför en patient har avböjt en planerad vårdkontakt'}", 'IsSpecialistReferral': "{'title_ui': 'Specialistvårdsrem', 'description': 'Om patienten har en specialistvårdsremiss från annan vårdgivare'}", 'SpecialistRefValidThroughDate': "{'title_ui': 'giltig tom', 'description': None}", 'IsAvailableOnShortNotice': "{'title_ui': 'Kort varsel', 'description': 'Om patienten kan komma med kort varsel'}", 'IsNeedingInterpreter': "{'title_ui': 'Tolk önskas', 'description': None}", 'InterpreterLanguage': "{'title_ui': 'språk', 'description': None}", 'Study': "{'title_ui': 'Forskning/studie', 'description': 'Ev. studie som patienten medverkar i'}", 'CarePlanComment': "{'title_ui': 'Kommentar', 'description': 'Visas i en vårdplanering, under Kontaktuppgifter'}", 'ConsultationDocumentID': "{'title_ui': 'Ärendenummer', 'description': None}", 'ConsultationDate': "{'title_ui': 'Remissdatum', 'description': None}", 'SerialNumber': "{'title_ui': None, 'description': 'Löpnummer som inte verkar användas'}", 'AppointmentID': "{'title_ui': None, 'description': 'Löpnummer som inte verkar användas'}", 'ActionGroupID': "{'title_ui': None, 'description': 'Kod som skickas till CVR. Är NULL i utvecklingsmiljön.'}", 'MunicipalityCompositeID': "{'title_ui': 'Län', 'description': 'Den kommun som patienten tillhör'}", 'OrdererUnitID': "{'title_ui': 'Produktionsområde', 'description': 'Id för den beställaravdelning som patienten tillhör'}", 'OrdererUnitName': "{'title_ui': 'Produktionsområde', 'description': 'Namn på beställaravdelning'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
		CAST([ActionGroupID] AS VARCHAR(MAX)) AS [ActionGroupID],
		CAST([AppointmentID] AS VARCHAR(MAX)) AS [AppointmentID],
		CAST([CarePlanComment] AS VARCHAR(MAX)) AS [CarePlanComment],
		CAST([CarePlanTypeID] AS VARCHAR(MAX)) AS [CarePlanTypeID],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CONVERT(varchar(max), [ConsultationDate], 126) AS [ConsultationDate],
		CAST([ConsultationDocumentID] AS VARCHAR(MAX)) AS [ConsultationDocumentID],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CONVERT(varchar(max), [DateCreated], 126) AS [DateCreated],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([InterpreterLanguage] AS VARCHAR(MAX)) AS [InterpreterLanguage],
		CAST([IsAvailableOnShortNotice] AS VARCHAR(MAX)) AS [IsAvailableOnShortNotice],
		CAST([IsNeedingInterpreter] AS VARCHAR(MAX)) AS [IsNeedingInterpreter],
		CAST([IsSpecialistReferral] AS VARCHAR(MAX)) AS [IsSpecialistReferral],
		CAST([ListStatusID] AS VARCHAR(MAX)) AS [ListStatusID],
		CAST([MunicipalityCompositeID] AS VARCHAR(MAX)) AS [MunicipalityCompositeID],
		CONVERT(varchar(max), [NotificationDatetime], 126) AS [NotificationDatetime],
		CAST([NotificationTypeID] AS VARCHAR(MAX)) AS [NotificationTypeID],
		CAST([NotifiedByUserID] AS VARCHAR(MAX)) AS [NotifiedByUserID],
		CAST([OrdererUnitID] AS VARCHAR(MAX)) AS [OrdererUnitID],
		CAST([OrdererUnitName] AS VARCHAR(MAX)) AS [OrdererUnitName],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PostponementReasonID] AS VARCHAR(MAX)) AS [PostponementReasonID],
		CAST([PriorityID] AS VARCHAR(MAX)) AS [PriorityID],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CONVERT(varchar(max), [RequestedDate], 126) AS [RequestedDate],
		CAST([ResponsibleUserID] AS VARCHAR(MAX)) AS [ResponsibleUserID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SectionKombika] AS VARCHAR(MAX)) AS [SectionKombika],
		CAST([SerialNumber] AS VARCHAR(MAX)) AS [SerialNumber],
		CONVERT(varchar(max), [SpecialistRefValidThroughDate], 126) AS [SpecialistRefValidThroughDate],
		CONVERT(varchar(max), [StartDate], 126) AS [StartDate],
		CAST([Study] AS VARCHAR(MAX)) AS [Study],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version],
		CAST([VisitReasonID] AS VARCHAR(MAX)) AS [VisitReasonID] 
	FROM Intelligence.viewreader.vCarePlans) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    