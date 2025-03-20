
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Remissmeddelanden för remiss",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CreationDate': 'varchar(max)', 'CreationTime': 'varchar(max)', 'DocumentID': 'varchar(max)', 'ExternalUnitIdTypeCode': 'varchar(max)', 'ForwardedToCareUnitExternalID': 'varchar(max)', 'LockGroupExternalID': 'varchar(max)', 'NoticeCode': 'varchar(max)', 'NotifyingCareUnitExternalID': 'varchar(max)', 'NotifyingPersonExternalHSAID': 'varchar(max)', 'NotifyingPersonExternalName': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferralVersionNumber': 'varchar(max)', 'ReferringCareUnitExternalID': 'varchar(max)', 'Row': 'varchar(max)', 'Text': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Unik rad för remissmeddelande'}", 'NoticeCode': "{'title_ui': None, 'description': 'Id för remissmeddelande'}", 'CreationDate': "{'title_ui': None, 'description': 'Datum för remissmeddelande'}", 'CreationTime': "{'title_ui': None, 'description': 'Tid för remissmeddelande'}", 'Type': "{'title_ui': None, 'description': {'break': [None, None, None]}}", 'Text': "{'title_ui': None, 'description': 'Meddelandetext'}", 'ExternalUnitIdTypeCode': "{'title_ui': None, 'description': 'Typ av id i ReferringCareUnitExternalID, NotifyingCareUnitExternalID och ForwardedToCareUnitExternalID'}", 'ReferringCareUnitExternalID': "{'title_ui': None, 'description': 'Remitterande enhets ID'}", 'NotifyingCareUnitExternalID': "{'title_ui': None, 'description': 'Den enhet som skickar bekräftelsen eller annat remissmeddelande'}", 'ForwardedToCareUnitExternalID': "{'title_ui': None, 'description': 'Den enhet till vilken remissen har vidarebefordrats'}", 'NotifyingPersonExternalHSAID': "{'title_ui': None, 'description': 'Avsändande persons HSAID'}", 'NotifyingPersonExternalName': "{'title_ui': None, 'description': 'Avsändande persons namn och titel'}", 'ReferralVersionNumber': "{'title_ui': None, 'description': 'Version av remissen remissmeddelandet tillhör'}", 'LockGroupExternalID': "{'title_ui': None, 'description': 'Extern spärrgrupp till vilken remissmeddelandet hör'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), CreationDate, 126) AS CreationDate,
		CONVERT(varchar(max), CreationTime, 126) AS CreationTime,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(ExternalUnitIdTypeCode AS VARCHAR(MAX)) AS ExternalUnitIdTypeCode,
		CAST(ForwardedToCareUnitExternalID AS VARCHAR(MAX)) AS ForwardedToCareUnitExternalID,
		CAST(LockGroupExternalID AS VARCHAR(MAX)) AS LockGroupExternalID,
		CAST(NoticeCode AS VARCHAR(MAX)) AS NoticeCode,
		CAST(NotifyingCareUnitExternalID AS VARCHAR(MAX)) AS NotifyingCareUnitExternalID,
		CAST(NotifyingPersonExternalHSAID AS VARCHAR(MAX)) AS NotifyingPersonExternalHSAID,
		CAST(NotifyingPersonExternalName AS VARCHAR(MAX)) AS NotifyingPersonExternalName,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReferralVersionNumber AS VARCHAR(MAX)) AS ReferralVersionNumber,
		CAST(ReferringCareUnitExternalID AS VARCHAR(MAX)) AS ReferringCareUnitExternalID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(Text AS VARCHAR(MAX)) AS Text,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Type AS VARCHAR(MAX)) AS Type,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vConsultRefs_Notices) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    