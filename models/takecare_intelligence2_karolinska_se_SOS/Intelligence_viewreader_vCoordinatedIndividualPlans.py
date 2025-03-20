
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdmissionDate': 'varchar(max)', 'AdmissionNoticeSentByUserID': 'varchar(max)', 'AdmissionNoticeSentDatetime': 'varchar(max)', 'AdmissionTime': 'varchar(max)', 'AdmittedToCareUnitID': 'varchar(max)', 'CareAssessmentPlan': 'varchar(max)', 'CareAssessmentPlanResponsibleUserID': 'varchar(max)', 'CareUnitAdministratorName': 'varchar(max)', 'CareUnitConfirmPlanDate': 'varchar(max)', 'CareUnitConfirmPlanTime': 'varchar(max)', 'CaseCompletedDate': 'varchar(max)', 'CaseNumber': 'varchar(max)', 'Comment': 'varchar(max)', 'CommentLastModified': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DischargeDate': 'varchar(max)', 'DischargeReadyDate': 'varchar(max)', 'DischargeReadyResponsibleDoctorName': 'varchar(max)', 'DischargeReadyTime': 'varchar(max)', 'DischargeReadyWithdrawnDate': 'varchar(max)', 'DischargeReadyWithdrawnTime': 'varchar(max)', 'DischargeTo': 'varchar(max)', 'DocumentID': 'varchar(max)', 'HasInvitedMunicipality': 'varchar(max)', 'HasInvitedOutpatientCare': 'varchar(max)', 'HasInvitedParticipants': 'varchar(max)', 'HasPatientConsentToSIPInvite': 'varchar(max)', 'HasPatientConsentToSendNotice': 'varchar(max)', 'InviteConsentUserResponsibleUserID': 'varchar(max)', 'InviteParticipants': 'varchar(max)', 'IsAdjustedByMunicipality': 'varchar(max)', 'IsAdjustedByOutpatient': 'varchar(max)', 'IsDeceased': 'varchar(max)', 'IsMeetingTimeConfirmedByCareUnit': 'varchar(max)', 'IsMeetingTimeConfirmedByMunicipality': 'varchar(max)', 'IsOptOrv': 'varchar(max)', 'IsPaperHandledPlan': 'varchar(max)', 'IsPlanReadyforAdjustment': 'varchar(max)', 'LinkedPASDocumentID': 'varchar(max)', 'MeetingDate': 'varchar(max)', 'MeetingInfo': 'varchar(max)', 'MeetingTime': 'varchar(max)', 'MeetingType': 'varchar(max)', 'MovedFromCareunit': 'varchar(max)', 'MunicipalityAdjustedAdministrator': 'varchar(max)', 'MunicipalityAdjustedDate': 'varchar(max)', 'MunicipalityAdministratorName': 'varchar(max)', 'MunicipalityConfirmPlanDate': 'varchar(max)', 'MunicipalityConfirmPlanTime': 'varchar(max)', 'MunicipalityID': 'varchar(max)', 'MunicipalityPlanInitiatives': 'varchar(max)', 'NewMeetingTimeCareUnit': 'varchar(max)', 'NewMeetingTimeMunicipality': 'varchar(max)', 'OutpatientAdjustedAdministrator': 'varchar(max)', 'OutpatientAdjustedDate': 'varchar(max)', 'OutpatientPlanInitiatives': 'varchar(max)', 'OutpatientRecipientID': 'varchar(max)', 'PatientAdmissionResponsibleUserID': 'varchar(max)', 'PatientConsentResponsibleUserID': 'varchar(max)', 'PatientConsentTypeToSendNoticeID': 'varchar(max)', 'PatientID': 'varchar(max)', 'PaymentLiabilityDate': 'varchar(max)', 'PermanentCareContactUserID': 'varchar(max)', 'PlanParticipants': 'varchar(max)', 'PlanSentDate': 'varchar(max)', 'PlanSentTime': 'varchar(max)', 'PlanStartUserID': 'varchar(max)', 'PlannedDischargeDate': 'varchar(max)', 'ReasonPlanNotInitiated': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'Status': 'varchar(max)', 'TimestampCoordinatedPlanStarted': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={},
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
		CONVERT(varchar(max), AdmissionDate, 126) AS AdmissionDate,
		CAST(AdmissionNoticeSentByUserID AS VARCHAR(MAX)) AS AdmissionNoticeSentByUserID,
		CONVERT(varchar(max), AdmissionNoticeSentDatetime, 126) AS AdmissionNoticeSentDatetime,
		CONVERT(varchar(max), AdmissionTime, 126) AS AdmissionTime,
		CAST(AdmittedToCareUnitID AS VARCHAR(MAX)) AS AdmittedToCareUnitID,
		CAST(CareAssessmentPlan AS VARCHAR(MAX)) AS CareAssessmentPlan,
		CAST(CareAssessmentPlanResponsibleUserID AS VARCHAR(MAX)) AS CareAssessmentPlanResponsibleUserID,
		CAST(CareUnitAdministratorName AS VARCHAR(MAX)) AS CareUnitAdministratorName,
		CONVERT(varchar(max), CareUnitConfirmPlanDate, 126) AS CareUnitConfirmPlanDate,
		CONVERT(varchar(max), CareUnitConfirmPlanTime, 126) AS CareUnitConfirmPlanTime,
		CONVERT(varchar(max), CaseCompletedDate, 126) AS CaseCompletedDate,
		CONVERT(varchar(max), CaseNumber, 126) AS CaseNumber,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CONVERT(varchar(max), CommentLastModified, 126) AS CommentLastModified,
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CONVERT(varchar(max), DischargeDate, 126) AS DischargeDate,
		CONVERT(varchar(max), DischargeReadyDate, 126) AS DischargeReadyDate,
		CAST(DischargeReadyResponsibleDoctorName AS VARCHAR(MAX)) AS DischargeReadyResponsibleDoctorName,
		CONVERT(varchar(max), DischargeReadyTime, 126) AS DischargeReadyTime,
		CONVERT(varchar(max), DischargeReadyWithdrawnDate, 126) AS DischargeReadyWithdrawnDate,
		CONVERT(varchar(max), DischargeReadyWithdrawnTime, 126) AS DischargeReadyWithdrawnTime,
		CAST(DischargeTo AS VARCHAR(MAX)) AS DischargeTo,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(HasInvitedMunicipality AS VARCHAR(MAX)) AS HasInvitedMunicipality,
		CAST(HasInvitedOutpatientCare AS VARCHAR(MAX)) AS HasInvitedOutpatientCare,
		CAST(HasInvitedParticipants AS VARCHAR(MAX)) AS HasInvitedParticipants,
		CAST(HasPatientConsentToSIPInvite AS VARCHAR(MAX)) AS HasPatientConsentToSIPInvite,
		CAST(HasPatientConsentToSendNotice AS VARCHAR(MAX)) AS HasPatientConsentToSendNotice,
		CAST(InviteConsentUserResponsibleUserID AS VARCHAR(MAX)) AS InviteConsentUserResponsibleUserID,
		CAST(InviteParticipants AS VARCHAR(MAX)) AS InviteParticipants,
		CAST(IsAdjustedByMunicipality AS VARCHAR(MAX)) AS IsAdjustedByMunicipality,
		CAST(IsAdjustedByOutpatient AS VARCHAR(MAX)) AS IsAdjustedByOutpatient,
		CAST(IsDeceased AS VARCHAR(MAX)) AS IsDeceased,
		CAST(IsMeetingTimeConfirmedByCareUnit AS VARCHAR(MAX)) AS IsMeetingTimeConfirmedByCareUnit,
		CAST(IsMeetingTimeConfirmedByMunicipality AS VARCHAR(MAX)) AS IsMeetingTimeConfirmedByMunicipality,
		CAST(IsOptOrv AS VARCHAR(MAX)) AS IsOptOrv,
		CAST(IsPaperHandledPlan AS VARCHAR(MAX)) AS IsPaperHandledPlan,
		CAST(IsPlanReadyforAdjustment AS VARCHAR(MAX)) AS IsPlanReadyforAdjustment,
		CAST(LinkedPASDocumentID AS VARCHAR(MAX)) AS LinkedPASDocumentID,
		CONVERT(varchar(max), MeetingDate, 126) AS MeetingDate,
		CAST(MeetingInfo AS VARCHAR(MAX)) AS MeetingInfo,
		CONVERT(varchar(max), MeetingTime, 126) AS MeetingTime,
		CAST(MeetingType AS VARCHAR(MAX)) AS MeetingType,
		CAST(MovedFromCareunit AS VARCHAR(MAX)) AS MovedFromCareunit,
		CAST(MunicipalityAdjustedAdministrator AS VARCHAR(MAX)) AS MunicipalityAdjustedAdministrator,
		CONVERT(varchar(max), MunicipalityAdjustedDate, 126) AS MunicipalityAdjustedDate,
		CAST(MunicipalityAdministratorName AS VARCHAR(MAX)) AS MunicipalityAdministratorName,
		CONVERT(varchar(max), MunicipalityConfirmPlanDate, 126) AS MunicipalityConfirmPlanDate,
		CONVERT(varchar(max), MunicipalityConfirmPlanTime, 126) AS MunicipalityConfirmPlanTime,
		CAST(MunicipalityID AS VARCHAR(MAX)) AS MunicipalityID,
		CAST(MunicipalityPlanInitiatives AS VARCHAR(MAX)) AS MunicipalityPlanInitiatives,
		CAST(NewMeetingTimeCareUnit AS VARCHAR(MAX)) AS NewMeetingTimeCareUnit,
		CAST(NewMeetingTimeMunicipality AS VARCHAR(MAX)) AS NewMeetingTimeMunicipality,
		CAST(OutpatientAdjustedAdministrator AS VARCHAR(MAX)) AS OutpatientAdjustedAdministrator,
		CONVERT(varchar(max), OutpatientAdjustedDate, 126) AS OutpatientAdjustedDate,
		CAST(OutpatientPlanInitiatives AS VARCHAR(MAX)) AS OutpatientPlanInitiatives,
		CAST(OutpatientRecipientID AS VARCHAR(MAX)) AS OutpatientRecipientID,
		CAST(PatientAdmissionResponsibleUserID AS VARCHAR(MAX)) AS PatientAdmissionResponsibleUserID,
		CAST(PatientConsentResponsibleUserID AS VARCHAR(MAX)) AS PatientConsentResponsibleUserID,
		CAST(PatientConsentTypeToSendNoticeID AS VARCHAR(MAX)) AS PatientConsentTypeToSendNoticeID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), PaymentLiabilityDate, 126) AS PaymentLiabilityDate,
		CAST(PermanentCareContactUserID AS VARCHAR(MAX)) AS PermanentCareContactUserID,
		CAST(PlanParticipants AS VARCHAR(MAX)) AS PlanParticipants,
		CONVERT(varchar(max), PlanSentDate, 126) AS PlanSentDate,
		CONVERT(varchar(max), PlanSentTime, 126) AS PlanSentTime,
		CAST(PlanStartUserID AS VARCHAR(MAX)) AS PlanStartUserID,
		CONVERT(varchar(max), PlannedDischargeDate, 126) AS PlannedDischargeDate,
		CAST(ReasonPlanNotInitiated AS VARCHAR(MAX)) AS ReasonPlanNotInitiated,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(Status AS VARCHAR(MAX)) AS Status,
		CONVERT(varchar(max), TimestampCoordinatedPlanStarted, 126) AS TimestampCoordinatedPlanStarted,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vCoordinatedIndividualPlans) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    