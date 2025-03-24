
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Info om de remissbekräftelsemallar som använts vid remissbekräftelse.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'ConfirmedDateTime': 'varchar(max)', 'DocumentID': 'varchar(max)', 'OutputFormatID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReferralNotificationMethodID': 'varchar(max)', 'Row': 'varchar(max)', 'TemplateID': 'varchar(max)', 'TemplateName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Unik rad för brevtyp'}", 'OutputFormatID': "{'title_ui': None, 'description': '0=TakeCare-brev 1=word-fil 2=pdf-fil'}", 'TemplateID': "{'title_ui': None, 'description': 'Mall-id. Komponentnumret i vårdenhetens brevmallsfil där mallen finns.'}", 'TemplateName': "{'title_ui': 'Dokument', 'description': 'Mallnamn'}", 'CareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Inloggad vårdenhet'}", 'ConfirmedDateTime': "{'title_ui': 'Bekräftad', 'description': 'Tidpunkt då remiss bekräftades.'}", 'ReferralNotificationMethodID': "{'title_ui': 'Bekräftelsesätt', 'description': 'Remissnotissätt. Metoden som användes för att skicka denna bekräftelse.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CONVERT(varchar(max), [ConfirmedDateTime], 126) AS [ConfirmedDateTime],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([OutputFormatID] AS VARCHAR(MAX)) AS [OutputFormatID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ReferralNotificationMethodID] AS VARCHAR(MAX)) AS [ReferralNotificationMethodID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([TemplateID] AS VARCHAR(MAX)) AS [TemplateID],
		CAST([TemplateName] AS VARCHAR(MAX)) AS [TemplateName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vConsultRefs_ConfirmationLetterTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    