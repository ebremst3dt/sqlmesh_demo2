
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Beställning Patologi/Cytologi Karolinska. Exakt hur data ser ut kan variera beroende på vilket gränssnitt som använts.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'BioBankConsentID': 'varchar(max)', 'CancerRegistration': 'varchar(max)', 'Comment': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'CytostaticsDate': 'varchar(max)', 'DocumentID': 'varchar(max)', 'EmergencyTelNo': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'HasFrozenSection': 'varchar(max)', 'HasHormoneTherapy': 'varchar(max)', 'HasMenopause': 'varchar(max)', 'HasPrevAbnormality': 'varchar(max)', 'HasRadioTherapy': 'varchar(max)', 'History': 'varchar(max)', 'HormoneTherapy': 'varchar(max)', 'InvoiceeCareUnit': 'varchar(max)', 'InvoiceeCareUnitExternalID': 'varchar(max)', 'InvoiceeCareUnitKombika': 'varchar(max)', 'IsBloodInfection': 'varchar(max)', 'IsEmergency': 'varchar(max)', 'IsHormoneReceptor': 'varchar(max)', 'IsPregnant': 'varchar(max)', 'LatestMestruation': 'varchar(max)', 'Liver': 'varchar(max)', 'Localization': 'varchar(max)', 'LymphGlands': 'varchar(max)', 'MComponent': 'varchar(max)', 'MenopauseYear': 'varchar(max)', 'NumberOfCells': 'varchar(max)', 'OrderDateTime': 'varchar(max)', 'OrderType': 'varchar(max)', 'OrderTypeID': 'varchar(max)', 'OrderedBy': 'varchar(max)', 'OrdererCareUnitExternalID': 'varchar(max)', 'OrdererCareUnitKombika': 'varchar(max)', 'Partus': 'varchar(max)', 'PatientID': 'varchar(max)', 'PatientWeight': 'varchar(max)', 'PlannedSamplingDate': 'varchar(max)', 'PlannedSamplingTime': 'varchar(max)', 'PreparationNature': 'varchar(max)', 'PreparationNatureCode': 'varchar(max)', 'PrevAbnormality': 'varchar(max)', 'PrevAbnormalityYear': 'varchar(max)', 'PreviousExam': 'varchar(max)', 'Questionnaire': 'varchar(max)', 'RID': 'varchar(max)', 'RadioTherapyYear': 'varchar(max)', 'ReferringDoctor': 'varchar(max)', 'ReferringDoctorUserID': 'varchar(max)', 'ReferringDoctorUserName': 'varchar(max)', 'RegistrationStatusID': 'varchar(max)', 'ReplyRecipientCareUnit': 'varchar(max)', 'ReplyRecipientCareUnitExternalID': 'varchar(max)', 'ReplyRecipientCareUnitID': 'varchar(max)', 'ReplyRecipientCareUnitKombika': 'varchar(max)', 'RequestedExam': 'varchar(max)', 'RequestedExamCode': 'varchar(max)', 'Sampler': 'varchar(max)', 'SamplingDate': 'varchar(max)', 'SamplingMethod': 'varchar(max)', 'SamplingTime': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SavedStatusID': 'varchar(max)', 'SignedBy': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'Spleen': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SavedAtCareUnitID': "{'title_ui': 'Version skapad på', 'description': None}", 'RegistrationStatusID': "{'title_ui': 'Status', 'description': {'break': [None, None, None, None]}}", 'SavedStatusID': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'Comment': "{'title_ui': 'Kommentar', 'description': None}", 'OrderTypeID': "{'title_ui': None, 'description': {'break': [None, None, None, None]}}", 'OrderType': "{'title_ui': None, 'description': 'Den typ av beställning som valts i huvudmenyn'}", 'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'SignedByUserID': "{'title_ui': 'Signerad av', 'description': 'Pnr för användare som signerat dokumentet. Finns ej för beställningar före 2007.'}", 'SignedBy': "{'title_ui': 'Signerad av', 'description': 'Namn på användare som signerat dokumentet. Finns ej för beställningar före 2007.'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering. Finns ej för beställningar före 2007.'}", 'OrderedBy': "{'title_ui': 'Framställd av', 'description': 'Framställd av användare'}", 'OrderDateTime': "{'title_ui': 'Beställd', 'description': None}", 'RID': "{'title_ui': 'R:', 'description': None}", 'ReferringDoctorUserID': "{'title_ui': 'Remittent', 'description': 'Pnr för remitterande läkare/vidimeringsansvarig.'}", 'ReferringDoctorUserName': "{'title_ui': 'Remittent', 'description': 'Användarnamn för remitterande läkare/vidimeringsansvarig'}", 'ReferringDoctor': "{'title_ui': 'Remittent', 'description': 'Namn på remitterande läkare'}", 'OrdererCareUnitKombika': "{'title_ui': 'B:', 'description': 'Kombikakod/EXID för beställare.'}", 'ReplyRecipientCareUnitID': "{'title_ui': 'S:', 'description': 'Svarsmottagare vårdenhet (Är ofta samma som beställande vårdenhet.)'}", 'ReplyRecipientCareUnitKombika': "{'title_ui': 'S:', 'description': 'Kombikakod/EXID för svarsmottagare'}", 'ReplyRecipientCareUnit': "{'title_ui': 'S:', 'description': 'Svarsmottagare'}", 'InvoiceeCareUnitKombika': "{'title_ui': 'F:', 'description': 'Kombikakod/EXID för fakturamottagare'}", 'InvoiceeCareUnit': "{'title_ui': 'F:', 'description': 'Fakturamottagare'}", 'PlannedSamplingDate': "{'title_ui': 'Planerad prov.datum.', 'description': 'Innehåller alltid planerad provt. tidpunkt. Finns ej för beställningar före 2007. Vid obduktion lagras datumet då patienten avled.'}", 'PlannedSamplingTime': "{'title_ui': 'Planerad prov.datum.', 'description': 'Innehåller alltid planerad provt. tidpunkt. Finns ej för beställningar före 2007. Vid obduktion lagras tiden då patienten avled.'}", 'SamplingDate': "{'title_ui': 'Provtagningstid/Patienten avliden', 'description': 'Provtagningstidpunkt. Vid obduktion lagras datumet då patienten avled.'}", 'SamplingTime': "{'title_ui': 'Provtagningstid/Patienten avliden', 'description': 'Provtagningstidpunkt. Vid obduktion lagras tiden då patienten avled.'}", 'PreparationNatureCode': "{'title_ui': 'Preparatets natur', 'description': 'Kod. Endast CD 34/Stamcellstranspl.'}", 'PreparationNature': "{'title_ui': 'Preparatets natur', 'description': 'Ej Obduktion'}", 'Questionnaire': "{'title_ui': 'Frågeställning alt. Diagnos/fråga', 'description': 'Ej CD 34/Stamcellstranspl.'}", 'History': "{'title_ui': 'Anamnes/Relevanta kliniska uppgifter/Klinisk epikris/Övriga uppgifter', 'description': None}", 'IsBloodInfection': "{'title_ui': 'Blodsmitta', 'description': None}", 'IsEmergency': "{'title_ui': 'Snabbsvar/Akutsvar', 'description': 'Ej Obduktion'}", 'EmergencyTelNo': "{'title_ui': 'telnr', 'description': 'Ej Obduktion'}", 'HasFrozenSection': "{'title_ui': 'Fryssnitt', 'description': 'Endast Pat/Cytologi'}", 'IsHormoneReceptor': "{'title_ui': 'Hormonreceptor', 'description': 'Endast Pat/Cytologi'}", 'PreviousExam': "{'title_ui': 'Föreg.pat/cyt nr', 'description': 'Ej CD 34/Stamcellstranspl. LID eller RID på tidigare undersökning.'}", 'LatestMestruation': "{'title_ui': 'Senaste mens', 'description': 'Endast Gynekologisk Cytologi'}", 'IsPregnant': "{'title_ui': 'Gravid', 'description': 'Endast Gynekologisk Cytologi'}", 'Partus': "{'title_ui': 'Partus', 'description': 'Endast Gynekologisk Cytologi'}", 'HasMenopause': "{'title_ui': 'Menopaus', 'description': 'Endast Gynekologisk Cytologi'}", 'MenopauseYear': "{'title_ui': 'Menopaus år', 'description': 'Endast Gynekologisk Cytologi'}", 'HasHormoneTherapy': "{'title_ui': 'Hormonterapi', 'description': 'Endast Gynekologisk Cytologi'}", 'HormoneTherapy': "{'title_ui': 'Hormonterapi vad', 'description': 'Endast Gynekologisk Cytologi'}", 'HasPrevAbnormality': "{'title_ui': 'Tidigare atypi', 'description': 'Endast Gynekologisk Cytologi'}", 'PrevAbnormalityYear': "{'title_ui': 'Tidigare atypi år', 'description': 'Endast Gynekologisk Cytologi'}", 'PrevAbnormality': "{'title_ui': 'Tidigare atypi var', 'description': 'Endast Gynekologisk Cytologi'}", 'HasRadioTherapy': "{'title_ui': 'Strålbehandlad', 'description': 'Endast Pat/Cytologi och Gynekologisk Cytologi'}", 'RadioTherapyYear': "{'title_ui': 'Strålbehandlad år', 'description': 'Endast Pat/Cytologi och Gynekologisk Cytologi'}", 'Localization': "{'title_ui': 'Provtagningsställe', 'description': 'Var på kroppen som provet tagits. Endast Benmärgsundersökning'}", 'SamplingMethod': "{'title_ui': 'Provtagningsteknik', 'description': 'Endast Benmärgsundersökning'}", 'Sampler': "{'title_ui': 'Provtagare', 'description': 'Endast Benmärgsundersökning'}", 'CytostaticsDate': "{'title_ui': 'Datum för senast givna cytostatika', 'description': 'Endast Benmärgsundersökning'}", 'LymphGlands': "{'title_ui': 'Lymfkörtlar', 'description': 'Endast Benmärgsundersökning'}", 'Liver': "{'title_ui': 'Lever', 'description': 'Endast Benmärgsundersökning'}", 'Spleen': "{'title_ui': 'Mjälte', 'description': 'Endast Benmärgsundersökning'}", 'MComponent': "{'title_ui': 'M-komponent, typ, storlek', 'description': 'Endast Benmärgsundersökning'}", 'CancerRegistration': "{'title_ui': 'Cancer registrering', 'description': 'Endast Obduktion'}", 'PatientWeight': "{'title_ui': 'Vikt (kg)', 'description': 'Endast CD 34/Stamcellstranspl.'}", 'RequestedExamCode': "{'title_ui': 'Önskad undersökning', 'description': 'Kod. Endast CD 34/Stamcellstranspl.'}", 'RequestedExam': "{'title_ui': 'Önskad undersökning', 'description': 'Klartext. Endast CD 34/Stamcellstranspl.'}", 'NumberOfCells': "{'title_ui': 'Totalt antal celler', 'description': 'Endast CD 34/Stamcellstranspl.'}", 'BioBankConsentID': "{'title_ui': 'Sparas i biobank', 'description': 'Biobankslagskod. Ej Obduktion, då lagras 99.'}", 'ExternalUnitIdTypeCode': "{'title_ui': 'Id-typ', 'description': 'Id-typ för beställningen'}", 'OrdererCareUnitExternalID': "{'title_ui': 'B:', 'description': 'Kod för extern enhet för beställare'}", 'ReplyRecipientCareUnitExternalID': "{'title_ui': 'S:', 'description': 'Kod för extern enhet för svarsmottagare'}", 'InvoiceeCareUnitExternalID': "{'title_ui': 'F:', 'description': 'Kod för extern enhet för fakturamottagare'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(BioBankConsentID AS VARCHAR(MAX)) AS BioBankConsentID,
		CAST(CancerRegistration AS VARCHAR(MAX)) AS CancerRegistration,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(CytostaticsDate AS VARCHAR(MAX)) AS CytostaticsDate,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(EmergencyTelNo AS VARCHAR(MAX)) AS EmergencyTelNo,
		CAST(ExternalUnitIdTypeCode AS VARCHAR(MAX)) AS ExternalUnitIdTypeCode,
		CAST(HasFrozenSection AS VARCHAR(MAX)) AS HasFrozenSection,
		CAST(HasHormoneTherapy AS VARCHAR(MAX)) AS HasHormoneTherapy,
		CAST(HasMenopause AS VARCHAR(MAX)) AS HasMenopause,
		CAST(HasPrevAbnormality AS VARCHAR(MAX)) AS HasPrevAbnormality,
		CAST(HasRadioTherapy AS VARCHAR(MAX)) AS HasRadioTherapy,
		CAST(History AS VARCHAR(MAX)) AS History,
		CAST(HormoneTherapy AS VARCHAR(MAX)) AS HormoneTherapy,
		CAST(InvoiceeCareUnit AS VARCHAR(MAX)) AS InvoiceeCareUnit,
		CAST(InvoiceeCareUnitExternalID AS VARCHAR(MAX)) AS InvoiceeCareUnitExternalID,
		CAST(InvoiceeCareUnitKombika AS VARCHAR(MAX)) AS InvoiceeCareUnitKombika,
		CAST(IsBloodInfection AS VARCHAR(MAX)) AS IsBloodInfection,
		CAST(IsEmergency AS VARCHAR(MAX)) AS IsEmergency,
		CAST(IsHormoneReceptor AS VARCHAR(MAX)) AS IsHormoneReceptor,
		CAST(IsPregnant AS VARCHAR(MAX)) AS IsPregnant,
		CAST(LatestMestruation AS VARCHAR(MAX)) AS LatestMestruation,
		CAST(Liver AS VARCHAR(MAX)) AS Liver,
		CAST(Localization AS VARCHAR(MAX)) AS Localization,
		CAST(LymphGlands AS VARCHAR(MAX)) AS LymphGlands,
		CAST(MComponent AS VARCHAR(MAX)) AS MComponent,
		CAST(MenopauseYear AS VARCHAR(MAX)) AS MenopauseYear,
		CAST(NumberOfCells AS VARCHAR(MAX)) AS NumberOfCells,
		CONVERT(varchar(max), OrderDateTime, 126) AS OrderDateTime,
		CAST(OrderType AS VARCHAR(MAX)) AS OrderType,
		CAST(OrderTypeID AS VARCHAR(MAX)) AS OrderTypeID,
		CAST(OrderedBy AS VARCHAR(MAX)) AS OrderedBy,
		CAST(OrdererCareUnitExternalID AS VARCHAR(MAX)) AS OrdererCareUnitExternalID,
		CAST(OrdererCareUnitKombika AS VARCHAR(MAX)) AS OrdererCareUnitKombika,
		CAST(Partus AS VARCHAR(MAX)) AS Partus,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PatientWeight AS VARCHAR(MAX)) AS PatientWeight,
		CONVERT(varchar(max), PlannedSamplingDate, 126) AS PlannedSamplingDate,
		CONVERT(varchar(max), PlannedSamplingTime, 126) AS PlannedSamplingTime,
		CAST(PreparationNature AS VARCHAR(MAX)) AS PreparationNature,
		CAST(PreparationNatureCode AS VARCHAR(MAX)) AS PreparationNatureCode,
		CAST(PrevAbnormality AS VARCHAR(MAX)) AS PrevAbnormality,
		CAST(PrevAbnormalityYear AS VARCHAR(MAX)) AS PrevAbnormalityYear,
		CAST(PreviousExam AS VARCHAR(MAX)) AS PreviousExam,
		CAST(Questionnaire AS VARCHAR(MAX)) AS Questionnaire,
		CAST(RID AS VARCHAR(MAX)) AS RID,
		CAST(RadioTherapyYear AS VARCHAR(MAX)) AS RadioTherapyYear,
		CAST(ReferringDoctor AS VARCHAR(MAX)) AS ReferringDoctor,
		CAST(ReferringDoctorUserID AS VARCHAR(MAX)) AS ReferringDoctorUserID,
		CAST(ReferringDoctorUserName AS VARCHAR(MAX)) AS ReferringDoctorUserName,
		CAST(RegistrationStatusID AS VARCHAR(MAX)) AS RegistrationStatusID,
		CAST(ReplyRecipientCareUnit AS VARCHAR(MAX)) AS ReplyRecipientCareUnit,
		CAST(ReplyRecipientCareUnitExternalID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitExternalID,
		CAST(ReplyRecipientCareUnitID AS VARCHAR(MAX)) AS ReplyRecipientCareUnitID,
		CAST(ReplyRecipientCareUnitKombika AS VARCHAR(MAX)) AS ReplyRecipientCareUnitKombika,
		CAST(RequestedExam AS VARCHAR(MAX)) AS RequestedExam,
		CAST(RequestedExamCode AS VARCHAR(MAX)) AS RequestedExamCode,
		CAST(Sampler AS VARCHAR(MAX)) AS Sampler,
		CONVERT(varchar(max), SamplingDate, 126) AS SamplingDate,
		CAST(SamplingMethod AS VARCHAR(MAX)) AS SamplingMethod,
		CONVERT(varchar(max), SamplingTime, 126) AS SamplingTime,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SavedStatusID AS VARCHAR(MAX)) AS SavedStatusID,
		CAST(SignedBy AS VARCHAR(MAX)) AS SignedBy,
		CAST(SignedByUserID AS VARCHAR(MAX)) AS SignedByUserID,
		CONVERT(varchar(max), SignedDatetime, 126) AS SignedDatetime,
		CAST(Spleen AS VARCHAR(MAX)) AS Spleen,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vPathologyCytologyOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    