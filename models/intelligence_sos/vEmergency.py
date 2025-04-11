
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Information om ett akutbesök. Patienter som kommer in till akuten skrivs in i TakeCares akutliggare och då skapas en rad i denna tabell. När patienten lämnar akuten avslutas besöket och den försvinner från akutliggaren, men akutuppgifterna sparas.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ArrivalDatetime': 'varchar(max)', 'ArrivalTypeTermID': 'varchar(max)', 'BLIPPCode': 'varchar(max)', 'CareNeedID': 'varchar(max)', 'ConsultedCareTeamID': 'varchar(max)', 'ConsultedCareUnitID': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CreatedByUserID': 'varchar(max)', 'DoctorVisitAtDatetime': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EmergencyDischargeComment': 'varchar(max)', 'EmergencyDischargeDatetime': 'varchar(max)', 'IdentificationTypeID': 'varchar(max)', 'IsAffectedByDisaster': 'varchar(max)', 'IsCancelled': 'varchar(max)', 'IsCompleted': 'varchar(max)', 'IsLikelyAdmission': 'varchar(max)', 'IsSecret': 'varchar(max)', 'LocationID': 'varchar(max)', 'MovedToCareUnitID': 'varchar(max)', 'MovedToOtherLocationID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PriorityID': 'varchar(max)', 'ProblemCauseTermID': 'varchar(max)', 'ProblemDescription': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ResponsibleCareTeamID': 'varchar(max)', 'ResponsibleCareUnitID': 'varchar(max)', 'ResponsibleDoctorUserID': 'varchar(max)', 'ResponsibleDoctorUserName': 'varchar(max)', 'ResponsibleNurseInDatetime': 'varchar(max)', 'ResponsibleNurseUserID': 'varchar(max)', 'ResponsibleNurseUserName': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'ToLocationDatetime': 'varchar(max)', 'TriageHistoryComment': 'varchar(max)', 'TriageHistoryPriorityID': 'varchar(max)', 'TriageTemplateID': 'varchar(max)', 'TriageTotalPriorityID': 'varchar(max)', 'TriageVitalsPriorityID': 'varchar(max)', 'Version': 'varchar(max)', 'VisitReasonTermID': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Ansvarig vårdenhet (fram till c:a 2005 den vårdenhet där dokumentet senast sparades). Den vårdenhet som behörighet utgår från.'}", 'IsCancelled': "{'title_ui': None, 'description': None}", 'TimestampCreated': "{'title_ui': None, 'description': 'Tidpunkt för påbörjan av dokumentets ursprungliga version.'}", 'CreatedByUserID': "{'title_ui': None, 'description': 'Användaren som skapade den första versionen'}", 'CreatedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': 'Den vårdenhet där dokumentet är skapat.'}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Den yrkesgrupp användaren som skapat dokumentet tillhör(de). Användarens aktuella yrkesgrupp återfinns i användarförteckningen.'}", 'IsAffectedByDisaster': "{'title_ui': 'Katastrof', 'description': 'Patienter med markeringen katastrof går att sortera fram och se på alla akutmottagningar. På så sätt kan man koordinera arbetet vid stora katastrofer när en stor grupp patienter slussas till olika mottagningar.'}", 'IsSecret': "{'title_ui': 'Upplysning får (ej) lämnas', 'description': 'Om personalen inte får lämna ut information om det aktuella besöket'}", 'ArrivalDatetime': "{'title_ui': 'Ankomstdatum', 'description': 'När patienten anlände till akutmottagningen.'}", 'ArrivalTypeTermID': "{'title_ui': 'Ankomstsätt', 'description': None}", 'IdentificationTypeID': "{'title_ui': 'Id-sätt', 'description': 'Identifikationssätt'}", 'VisitReasonTermID': "{'title_ui': 'Besöksorsak', 'description': 'Det problem som föranledde akutbesöket, t.ex. yrsel.'}", 'ProblemCauseTermID': "{'title_ui': None, 'description': 'Orsak till det problem som ledde till besöket, t.ex. cykelolycka'}", 'ProblemDescription': "{'title_ui': 'Kommentar', 'description': 'Närmare beskrivning av besöksorsaken eller bara en kommentar.'}", 'IsLikelyAdmission': "{'title_ui': 'Trolig inskrivning', 'description': 'Sant om patienten troligen måste skrivas in.'}", 'CareNeedID': "{'title_ui': 'Omvårdnadsbehov', 'description': None}", 'PriorityID': "{'title_ui': 'Prioritet', 'description': None}", 'LocationID': "{'title_ui': 'Plats', 'description': None}", 'ResponsibleDoctorUserID': "{'title_ui': 'Läkare', 'description': 'Fr.o.m. sommaren 2007, tidigare lagrades endast användarnamn'}", 'ResponsibleDoctorUserName': "{'title_ui': 'Läkare', 'description': 'Ansvarig läkare (användarnamn)'}", 'ResponsibleNurseUserID': "{'title_ui': 'Sjuksköterska', 'description': 'Ansvarig sjuksköterska. Fr.o.m. sommaren 2007, tidigare lagrades endast användarnamn'}", 'ResponsibleNurseUserName': "{'title_ui': 'Sjuksköterska', 'description': 'Sjuksköterska (användarnamn)'}", 'ToLocationDatetime': "{'title_ui': 'Till plats', 'description': 'Sätts varje gång man ändrar plats för patienten, men kan också anges manuellt. Man får i allmänhet inte en plats omedelbart när man anländer.'}", 'DoctorVisitAtDatetime': "{'title_ui': 'Läkare in', 'description': 'Sätts första gången man anger en ansvarig läkare, men kan också anges manuellt.'}", 'ResponsibleNurseInDatetime': "{'title_ui': 'Sjuksköterska in', 'description': 'Sätts varje gång användaren ändrar Sjuksköterska.'}", 'ResponsibleCareUnitID': "{'title_ui': 'Ansvarig vårdenhet', 'description': None}", 'ResponsibleCareTeamID': "{'title_ui': 'Vårdlag', 'description': 'Ansvarigt vårdlag'}", 'ConsultedCareUnitID': "{'title_ui': 'Konsulterad vårdenhet', 'description': None}", 'ConsultedCareTeamID': "{'title_ui': 'Vårdlag', 'description': 'Konsulterat vårdlag'}", 'EmergencyDischargeDatetime': "{'title_ui': 'Ut från akutmottagningen/Datum', 'description': 'När patienten lämnade akuten'}", 'EmergencyDischargeComment': '{\'title_ui\': \'Ut från akutmottagningen/Kommentar\', \'description\': \'Kort kommentar till varför eller hur patienten lämnade akuten, t.ex. "Avviker" eller "Gick hem".\'}', 'MovedToCareUnitID': '{\'title_ui\': \'Ut från akutmottagningen/Till\', \'description\': \'Om patienten flyttas till en vårdenhet inom systemet anges denna här. Annars anges vart patienten tar vägen efter akutbesöket i "Ut till annan plats".\'}', 'MovedToOtherLocationID': "{'title_ui': 'Ut från akutmottagningen/Till', 'description': {'break': [None, None, None, None]}}", 'IsCompleted': "{'title_ui': 'Avslutad', 'description': 'Om patienten tagits bort från akutliggaren'}", 'RegistrationStatusID': "{'title_ui': 'Avslutad', 'description': {'break': [None, None, None]}}", 'BLIPPCode': "{'title_ui': 'BLIPP-kod', 'description': {'break': [None, None, None]}}", 'TriageTemplateID': "{'title_ui': 'Triagemall', 'description': 'Id för mätvärdesmallen'}", 'TriageTotalPriorityID': "{'title_ui': 'Triageprioritet', 'description': 'Id för total triageprioritet'}", 'TriageVitalsPriorityID': "{'title_ui': 'Vitalparametrar - prioritet', 'description': 'Id för prioritet vitalparametrar'}", 'TriageHistoryComment': "{'title_ui': 'Vitalhistoria - kommentar', 'description': 'Kommentar i vitalhistoria'}", 'TriageHistoryPriorityID': "{'title_ui': 'Vitalhistoria - prioritet', 'description': 'Prioritet i vitalhistoria'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['DocumentID', 'PatientID', 'Version']
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
		CONVERT(varchar(max), [ArrivalDatetime], 126) AS [ArrivalDatetime],
		CAST([ArrivalTypeTermID] AS VARCHAR(MAX)) AS [ArrivalTypeTermID],
		CAST([BLIPPCode] AS VARCHAR(MAX)) AS [BLIPPCode],
		CAST([CareNeedID] AS VARCHAR(MAX)) AS [CareNeedID],
		CAST([ConsultedCareTeamID] AS VARCHAR(MAX)) AS [ConsultedCareTeamID],
		CAST([ConsultedCareUnitID] AS VARCHAR(MAX)) AS [ConsultedCareUnitID],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([CreatedByUserID] AS VARCHAR(MAX)) AS [CreatedByUserID],
		CONVERT(varchar(max), [DoctorVisitAtDatetime], 126) AS [DoctorVisitAtDatetime],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([EmergencyDischargeComment] AS VARCHAR(MAX)) AS [EmergencyDischargeComment],
		CONVERT(varchar(max), [EmergencyDischargeDatetime], 126) AS [EmergencyDischargeDatetime],
		CAST([IdentificationTypeID] AS VARCHAR(MAX)) AS [IdentificationTypeID],
		CAST([IsAffectedByDisaster] AS VARCHAR(MAX)) AS [IsAffectedByDisaster],
		CAST([IsCancelled] AS VARCHAR(MAX)) AS [IsCancelled],
		CAST([IsCompleted] AS VARCHAR(MAX)) AS [IsCompleted],
		CAST([IsLikelyAdmission] AS VARCHAR(MAX)) AS [IsLikelyAdmission],
		CAST([IsSecret] AS VARCHAR(MAX)) AS [IsSecret],
		CAST([LocationID] AS VARCHAR(MAX)) AS [LocationID],
		CAST([MovedToCareUnitID] AS VARCHAR(MAX)) AS [MovedToCareUnitID],
		CAST([MovedToOtherLocationID] AS VARCHAR(MAX)) AS [MovedToOtherLocationID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PriorityID] AS VARCHAR(MAX)) AS [PriorityID],
		CAST([ProblemCauseTermID] AS VARCHAR(MAX)) AS [ProblemCauseTermID],
		CAST([ProblemDescription] AS VARCHAR(MAX)) AS [ProblemDescription],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([RegistrationStatusID] AS VARCHAR(MAX)) AS [RegistrationStatusID],
		CAST([ResponsibleCareTeamID] AS VARCHAR(MAX)) AS [ResponsibleCareTeamID],
		CAST([ResponsibleCareUnitID] AS VARCHAR(MAX)) AS [ResponsibleCareUnitID],
		CAST([ResponsibleDoctorUserID] AS VARCHAR(MAX)) AS [ResponsibleDoctorUserID],
		CAST([ResponsibleDoctorUserName] AS VARCHAR(MAX)) AS [ResponsibleDoctorUserName],
		CONVERT(varchar(max), [ResponsibleNurseInDatetime], 126) AS [ResponsibleNurseInDatetime],
		CAST([ResponsibleNurseUserID] AS VARCHAR(MAX)) AS [ResponsibleNurseUserID],
		CAST([ResponsibleNurseUserName] AS VARCHAR(MAX)) AS [ResponsibleNurseUserName],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CONVERT(varchar(max), [ToLocationDatetime], 126) AS [ToLocationDatetime],
		CAST([TriageHistoryComment] AS VARCHAR(MAX)) AS [TriageHistoryComment],
		CAST([TriageHistoryPriorityID] AS VARCHAR(MAX)) AS [TriageHistoryPriorityID],
		CAST([TriageTemplateID] AS VARCHAR(MAX)) AS [TriageTemplateID],
		CAST([TriageTotalPriorityID] AS VARCHAR(MAX)) AS [TriageTotalPriorityID],
		CAST([TriageVitalsPriorityID] AS VARCHAR(MAX)) AS [TriageVitalsPriorityID],
		CAST([Version] AS VARCHAR(MAX)) AS [Version],
		CAST([VisitReasonTermID] AS VARCHAR(MAX)) AS [VisitReasonTermID] 
	FROM Intelligence.viewreader.vEmergency) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    