
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Ett bokningsunderlag i TakeCare är en slags rapport som sammanställer data från en eller flera bokningar med samma AppointmentID. I denna tabell lagras alla bokningar direkt. Man kan alltså för en patient göra flera bokningar av olika resurser, t.ex. ett rum och en läkare, alla med ett bokningsunderlag. Även patienter som ligger på väntelistan (se Status) lagras som bokningar, men då alltid som en enda bokning.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AgentID': 'varchar(max)', 'AgentIDType': 'varchar(max)', 'AppointmentBasisTimestampCreated': 'varchar(max)', 'AppointmentID': 'varchar(max)', 'AppointmentTypeCategoryID': 'varchar(max)', 'BelongsToTeamNumber': 'varchar(max)', 'BillingSection': 'varchar(max)', 'Comment': 'varchar(max)', 'CounterComment': 'varchar(max)', 'HealthCareArea': 'varchar(max)', 'HealthCarePlanDocumentID': 'varchar(max)', 'InterpreterLanguage': 'varchar(max)', 'IsAvailableOnShortNotice': 'varchar(max)', 'IsBookingSelfCheckin': 'varchar(max)', 'IsCompleted': 'varchar(max)', 'IsExtraTime': 'varchar(max)', 'IsFeeFree': 'varchar(max)', 'IsFirstVisit': 'varchar(max)', 'IsNeedingInterpreter': 'varchar(max)', 'IsSpecialistReferral': 'varchar(max)', 'LongReferralArrivalDate': 'varchar(max)', 'MunicipalityCompositeID': 'varchar(max)', 'NotificationTypeID': 'varchar(max)', 'OrderInTeam': 'varchar(max)', 'Origin': 'varchar(max)', 'PatientComment': 'varchar(max)', 'PatientID': 'varchar(max)', 'PostponementReasonComment': 'varchar(max)', 'PostponementReasonID': 'varchar(max)', 'PriorityID': 'varchar(max)', 'ReferralArrivalDate': 'varchar(max)', 'ReferralDate': 'varchar(max)', 'ReferralDocumentID': 'varchar(max)', 'RequestedAppointmentDate': 'varchar(max)', 'RequestedAppointmentTime': 'varchar(max)', 'RequestedPersonResourceID': 'varchar(max)', 'ReservationDate': 'varchar(max)', 'ReservationTime': 'varchar(max)', 'ResourceID': 'varchar(max)', 'Row': 'varchar(max)', 'SMSNotificationTemplateID': 'varchar(max)', 'SMSTemplateSavedByUserID': 'varchar(max)', 'SMSTemplateSavedTimestamp': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SelfcheckinCode': 'varchar(max)', 'SlotLength': 'varchar(max)', 'SpecialistRefValidThroughDate': 'varchar(max)', 'StatusID': 'varchar(max)', 'Study': 'varchar(max)', 'TaskDecisionDate': 'varchar(max)', 'TimePriority': 'varchar(max)', 'TimeTypeID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'VisitReasonID': 'varchar(max)', 'VisitTypeID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'AppointmentID': "{'title_ui': 'Bokningsnummer', 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ResourceID': "{'title_ui': 'Resurs', 'description': 'Den resurs som bokningen gäller'}", 'ReservationDate': "{'title_ui': 'Datum', 'description': 'Bokat datum'}", 'ReservationTime': "{'title_ui': 'Tid', 'description': 'Bokad tid'}", 'StatusID': '{\'title_ui\': \'Status\', \'description\': \'Status för bokningen. Status "utebliven" sätts inte alltid, utan kan beräknas utifrån bokat datum och om patienten inte har kommit.\'}', 'TimeTypeID': "{'title_ui': 'Tidstyp', 'description': 'Vilken typ av aktivitet som ska ske denna tid, t.ex mottagning eller rond.'}", 'IsExtraTime': "{'title_ui': 'Extra tid', 'description': 'Sant om bokningen inte använder en tid som är definierad för resursen utan skapade en extra tid.'}", 'AppointmentBasisTimestampCreated': "{'title_ui': None, 'description': 'När bokningsunderlaget skapades. Ny från version 13.5.'}", 'TimestampCreated': "{'title_ui': None, 'description': 'När bokningen skapades.'}", 'TimestampSaved': "{'title_ui': 'Senast sparad', 'description': {'break': [None, None]}}", 'SavedByUserID': "{'title_ui': 'Senast sparad av', 'description': {'break': None}}", 'SavedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': None}", 'HealthCarePlanDocumentID': "{'title_ui': 'Vårdplans id', 'description': 'Om bokningsunderlaget genererats från en vårdplan finns dokumentid för denna vårdplan här.'}", 'RequestedPersonResourceID': "{'title_ui': 'Önskad bemanning', 'description': 'Om patienten är eller har varit på väntelista och önskad resurs har typen Befattning finns här vem man önskar ska bemanna befattningen.'}", 'PostponementReasonID': "{'title_ui': 'Framflyttningsorsak/ Ombokningsorsak/ Avbokningsorsak', 'description': {'break': [None, None]}}", 'PostponementReasonComment': "{'title_ui': 'Kommentar för framflyttning/ Kommentar för ombokning/ Kommentar för avbokning', 'description': 'Kommentar användaren gav till PostponementReasonID. Följer samma logik som den kolumnen.'}", 'RequestedAppointmentDate': "{'title_ui': 'Önskat datum', 'description': 'Om patienten är eller har varit på väntelista är det här det datum när man önskar boka patienten.'}", 'RequestedAppointmentTime': "{'title_ui': 'Önskat datum', 'description': 'Om patienten är eller har varit på väntelista är det här den tid när man önskar boka patienten.'}", 'PriorityID': "{'title_ui': 'Prioritet', 'description': 'Hur prioriterad patienten är på väntelistan.'}", 'NotificationTypeID': "{'title_ui': 'Kallelsesätt', 'description': 'Hur patienten ska kallas.'}", 'ReferralDocumentID': "{'title_ui': 'Ärendenummer', 'description': 'Id för den konsultremiss som föranledde bokningen'}", 'ReferralDate': "{'title_ui': 'Remissdatum', 'description': {'break': [None, None]}}", 'ReferralArrivalDate': "{'title_ui': 'Ankomstdatum', 'description': 'När remissen ankom, om någon finns'}", 'LongReferralArrivalDate': "{'title_ui': 'Långremissdatum', 'description': 'Ankomstdatum för långremiss'}", 'MunicipalityCompositeID': "{'title_ui': 'Län/Kommun', 'description': 'Det län som patienten var skriven i när bokningen skedde'}", 'HealthCareArea': "{'title_ui': 'Produktionsområde', 'description': 'Det sjukvårdsområde som patienten hörde till när bokningen skedde'}", 'BillingSection': "{'title_ui': 'Ekonomisk enhet', 'description': 'Ekonomisk enhet för kassan (kombikakod/EXID)'}", 'VisitTypeID': "{'title_ui': 'Besökstyp', 'description': None}", 'IsFirstVisit': "{'title_ui': '1:a besök', 'description': 'Indikerar om besöket är ett 1:a besök enligt vårdgarantin.'}", 'VisitReasonID': "{'title_ui': 'Besöksorsak', 'description': None}", 'IsSpecialistReferral': "{'title_ui': 'Specialistvårdsrem', 'description': 'Om patienten har specialistvårdsremiss eller inte'}", 'SpecialistRefValidThroughDate': "{'title_ui': 'Specialistvårdsremiss giltig tom', 'description': None}", 'IsAvailableOnShortNotice': "{'title_ui': 'Kort varsel', 'description': 'Om patienten kan komma med kort varsel, så hon kan kallas om en tid blir ledig med kort varsel.'}", 'IsNeedingInterpreter': "{'title_ui': 'Tolk önskas', 'description': None}", 'InterpreterLanguage': "{'title_ui': 'språk', 'description': None}", 'TaskDecisionDate': "{'title_ui': 'Beslut om aktivitet', 'description': None}", 'Study': "{'title_ui': 'Forskning/studie', 'description': 'Den studie patienten är med i, om någon'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar i bokning ej synlig i kassan'}", 'CounterComment': "{'title_ui': 'Kommentar till kassan', 'description': 'Kommentar i bokning synlig i kassan'}", 'PatientComment': "{'title_ui': 'Patientkommentar (webb)', 'description': 'Patientens kommentar via webben'}", 'Origin': "{'title_ui': 'Bokad från', 'description': 'Bokningens ursprung 0=TakeCare 1=Webben'}", 'IsFeeFree': "{'title_ui': 'Avgiftsfritt', 'description': 'Avgiftsfritt'}", 'IsBookingSelfCheckin': "{'title_ui': 'Självincheckningsbar bokning', 'description': 'Om bokningen tillåter patienten att självinchecka'}", 'SelfcheckinCode': "{'title_ui': 'Självincheckningskod', 'description': 'Kod som patienten måste använda för att självinchecka på terminalen'}", 'SMSTemplateSavedByUserID': "{'title_ui': 'SMS senast ändrad av', 'description': 'Användare som, via bokningsunderlag, senast valde och sparade SMS-mall i bokningen.'}", 'SMSTemplateSavedTimestamp': "{'title_ui': 'SMS senast ändrad av', 'description': 'Datum då SMS-mall, via bokningsunderlag, senast valdes och sparades i bokningen.'}", 'AgentIDType': "{'title_ui': None, 'description': 'Typ av id för ombud vid nybokning via webben: 1=HSAID'}", 'AgentID': "{'title_ui': 'Bokad via ombud', 'description': 'Id för ombud vid nybokning via webben'}", 'SlotLength': "{'title_ui': None, 'description': 'Längd i minuter på bokningen'}", 'AppointmentTypeCategoryID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'BelongsToTeamNumber': "{'title_ui': None, 'description': 'Ny 2009-12-08. Alla bokningar (i ett bokningsunderlag) som hör ihop med ett team har samma nummer. Sätts till 0 om kolumn AppointmentTypeCategoryID är noll. Värdet är ej definierat för AppointmentTypeCategoryID=1 eller 3.'}", 'OrderInTeam': "{'title_ui': None, 'description': 'Ny 2009-12-08. Sortering av bokningar inom en grupp. Sätts till 0 om kolumn AppointmentTypeCategoryID är 0.'}", 'IsCompleted': "{'title_ui': 'Sätt som klar/inte klar', 'description': 'Ny 2009-12-08. Enheten bestämmer själv vad denna flagga ska användas till.'}", 'SMSNotificationTemplateID': "{'title_ui': 'SMS-påminnelse', 'description': 'Ny 2010-06-08. Id på SMS-mallen.'}", 'TimePriority': "{'title_ui': 'Tidsprioritet', 'description': 'Tidsprioritet för väntelistepost'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([AgentID] AS VARCHAR(MAX)) AS [AgentID],
		CAST([AgentIDType] AS VARCHAR(MAX)) AS [AgentIDType],
		CONVERT(varchar(max), [AppointmentBasisTimestampCreated], 126) AS [AppointmentBasisTimestampCreated],
		CAST([AppointmentID] AS VARCHAR(MAX)) AS [AppointmentID],
		CAST([AppointmentTypeCategoryID] AS VARCHAR(MAX)) AS [AppointmentTypeCategoryID],
		CAST([BelongsToTeamNumber] AS VARCHAR(MAX)) AS [BelongsToTeamNumber],
		CAST([BillingSection] AS VARCHAR(MAX)) AS [BillingSection],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([CounterComment] AS VARCHAR(MAX)) AS [CounterComment],
		CAST([HealthCareArea] AS VARCHAR(MAX)) AS [HealthCareArea],
		CAST([HealthCarePlanDocumentID] AS VARCHAR(MAX)) AS [HealthCarePlanDocumentID],
		CAST([InterpreterLanguage] AS VARCHAR(MAX)) AS [InterpreterLanguage],
		CAST([IsAvailableOnShortNotice] AS VARCHAR(MAX)) AS [IsAvailableOnShortNotice],
		CAST([IsBookingSelfCheckin] AS VARCHAR(MAX)) AS [IsBookingSelfCheckin],
		CAST([IsCompleted] AS VARCHAR(MAX)) AS [IsCompleted],
		CAST([IsExtraTime] AS VARCHAR(MAX)) AS [IsExtraTime],
		CAST([IsFeeFree] AS VARCHAR(MAX)) AS [IsFeeFree],
		CAST([IsFirstVisit] AS VARCHAR(MAX)) AS [IsFirstVisit],
		CAST([IsNeedingInterpreter] AS VARCHAR(MAX)) AS [IsNeedingInterpreter],
		CAST([IsSpecialistReferral] AS VARCHAR(MAX)) AS [IsSpecialistReferral],
		CONVERT(varchar(max), [LongReferralArrivalDate], 126) AS [LongReferralArrivalDate],
		CAST([MunicipalityCompositeID] AS VARCHAR(MAX)) AS [MunicipalityCompositeID],
		CAST([NotificationTypeID] AS VARCHAR(MAX)) AS [NotificationTypeID],
		CAST([OrderInTeam] AS VARCHAR(MAX)) AS [OrderInTeam],
		CAST([Origin] AS VARCHAR(MAX)) AS [Origin],
		CAST([PatientComment] AS VARCHAR(MAX)) AS [PatientComment],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PostponementReasonComment] AS VARCHAR(MAX)) AS [PostponementReasonComment],
		CAST([PostponementReasonID] AS VARCHAR(MAX)) AS [PostponementReasonID],
		CAST([PriorityID] AS VARCHAR(MAX)) AS [PriorityID],
		CONVERT(varchar(max), [ReferralArrivalDate], 126) AS [ReferralArrivalDate],
		CONVERT(varchar(max), [ReferralDate], 126) AS [ReferralDate],
		CAST([ReferralDocumentID] AS VARCHAR(MAX)) AS [ReferralDocumentID],
		CONVERT(varchar(max), [RequestedAppointmentDate], 126) AS [RequestedAppointmentDate],
		CONVERT(varchar(max), [RequestedAppointmentTime], 126) AS [RequestedAppointmentTime],
		CAST([RequestedPersonResourceID] AS VARCHAR(MAX)) AS [RequestedPersonResourceID],
		CONVERT(varchar(max), [ReservationDate], 126) AS [ReservationDate],
		CONVERT(varchar(max), [ReservationTime], 126) AS [ReservationTime],
		CAST([ResourceID] AS VARCHAR(MAX)) AS [ResourceID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SMSNotificationTemplateID] AS VARCHAR(MAX)) AS [SMSNotificationTemplateID],
		CAST([SMSTemplateSavedByUserID] AS VARCHAR(MAX)) AS [SMSTemplateSavedByUserID],
		CONVERT(varchar(max), [SMSTemplateSavedTimestamp], 126) AS [SMSTemplateSavedTimestamp],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SelfcheckinCode] AS VARCHAR(MAX)) AS [SelfcheckinCode],
		CAST([SlotLength] AS VARCHAR(MAX)) AS [SlotLength],
		CONVERT(varchar(max), [SpecialistRefValidThroughDate], 126) AS [SpecialistRefValidThroughDate],
		CAST([StatusID] AS VARCHAR(MAX)) AS [StatusID],
		CAST([Study] AS VARCHAR(MAX)) AS [Study],
		CONVERT(varchar(max), [TaskDecisionDate], 126) AS [TaskDecisionDate],
		CAST([TimePriority] AS VARCHAR(MAX)) AS [TimePriority],
		CAST([TimeTypeID] AS VARCHAR(MAX)) AS [TimeTypeID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([VisitReasonID] AS VARCHAR(MAX)) AS [VisitReasonID],
		CAST([VisitTypeID] AS VARCHAR(MAX)) AS [VisitTypeID] 
	FROM Intelligence.viewreader.vAppointments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    