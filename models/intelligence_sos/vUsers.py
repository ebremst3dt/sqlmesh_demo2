
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Information om de registrerade användarna.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CaseSensitivePassword': 'varchar(max)', 'Comment': 'varchar(max)', 'EmploymentCareUnitGroupID': 'varchar(max)', 'HSAID': 'varchar(max)', 'HSAIDFullLength': 'varchar(max)', 'HSAIDTypeID': 'varchar(max)', 'HasUserBillingPrivs': 'varchar(max)', 'HasUserCareUnitGroupPrivs': 'varchar(max)', 'HasUserCareUnitGroupPrivsAdm': 'varchar(max)', 'HasUserCareUnitPrivs': 'varchar(max)', 'HasUserProfiles': 'varchar(max)', 'InactivationDate': 'varchar(max)', 'IsSystem': 'varchar(max)', 'LastLoginDateTime': 'varchar(max)', 'LastPasswordChangeDate': 'varchar(max)', 'LastReceivedTicketDateTime': 'varchar(max)', 'LoginType': 'varchar(max)', 'Name': 'varchar(max)', 'ProfessionID': 'varchar(max)', 'SavedByName': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'UserID': 'varchar(max)', 'UserName': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'Version': 'varchar(max)', 'VersionUserID': 'varchar(max)'},
    column_descriptions={'UserID': "{'title_ui': 'Personnr', 'description': 'Användarens personnummer'}", 'Version': "{'title_ui': None, 'description': 'Version av uppgifterna'}", 'TimestampSaved': "{'title_ui': 'Senast ändrad', 'description': 'När denna version sparades'}", 'VersionUserID': "{'title_ui': None, 'description': 'Användarens personnummer i denna version (ska inte ändras)'}", 'Name': "{'title_ui': 'Förnamn Efternamn', 'description': 'Användarens namn'}", 'UserName': "{'title_ui': 'Användarnamn', 'description': 'Användarens användarnamn'}", 'SavedByName': "{'title_ui': 'Uppgifter senast ändrade av', 'description': 'Den som skapade denna version'}", 'SavedByUserID': "{'title_ui': 'Uppgifter senast ändrade av', 'description': 'Den som skapade denna version (personnummer)'}", 'Comment': "{'title_ui': 'Anteckningar', 'description': 'Kommentar som administratör skrivit in'}", 'ProfessionID': "{'title_ui': 'Yrkesgrupp', 'description': 'Yrkesgruppskod för primärrollen'}", 'EmploymentCareUnitGroupID': "{'title_ui': 'Anställd på vårdenhetsgrupp i primärrollen', 'description': None}", 'LastPasswordChangeDate': "{'title_ui': None, 'description': 'När användaren senast ändrade sitt lösenord'}", 'HSAID': "{'title_ui': None, 'description': 'Användarens HSA-id'}", 'HSAIDTypeID': "{'title_ui': 'HSA-id typ', 'description': {'break': None}}", 'HSAIDFullLength': '{\'title_ui\': \'HSA-id\', \'description\': \'Användarens kompletta HSA-id. Används om "Separat fält för HSAid per användare" är aktiverat i Generella systemparametrar.\'}', 'InactivationDate': "{'title_ui': None, 'description': 'Det datum användaren inte längre kan logga in i TakeCare'}", 'ValidFromDate': "{'title_ui': 'Giltig fr.o.m.', 'description': 'Tidigaste giltig fr.o.m. datum i någon av rollerna'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Senaste giltig t.o.m. datum i någon av rollerna'}", 'CaseSensitivePassword': "{'title_ui': None, 'description': 'Användaren har skiftlägeskänslig lösenordsinmatning. Ny funktion från 200810 som tas i bruk när användaren byter lösenord.'}", 'LoginType': "{'title_ui': 'Inloggningssätt', 'description': {'break': [None, None, None]}}", 'LastLoginDateTime': "{'title_ui': None, 'description': 'Senaste tidpunkt då användaren loggade in eller när kontot aktiverades av en administratör (oavsett inloggningssätt). Detta data uppdateras inte vid varje inloggning för systemkonton av prestandaskäl.'}", 'LastReceivedTicketDateTime': "{'title_ui': None, 'description': 'Senaste tidpunkt då artefakt växlades mot SSO-biljett. Sker vid inloggning i TakeCare via SLL Navigator eller Kortinloggning.'}", 'HasUserCareUnitPrivs': "{'title_ui': None, 'description': 'Användaren har behörighet till minst en vårdenhet i någon roll.'}", 'HasUserBillingPrivs': "{'title_ui': None, 'description': 'Användaren har behörighet till minst en kassa i någon roll.'}", 'HasUserCareUnitGroupPrivs': "{'title_ui': None, 'description': 'Användaren har behörighet till minst en vårdenhetsgrupp i någon roll.'}", 'HasUserProfiles': "{'title_ui': None, 'description': 'Användaren har minst en behörighetsprofil i någon roll.'}", 'HasUserCareUnitGroupPrivsAdm': "{'title_ui': None, 'description': 'Användaren har i någon roll rätt att ge andra behörighet till minst en vårdenhetsgrupp.'}", 'IsSystem': "{'title_ui': None, 'description': 'Användaren är ett system'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CaseSensitivePassword] AS VARCHAR(MAX)) AS [CaseSensitivePassword],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([EmploymentCareUnitGroupID] AS VARCHAR(MAX)) AS [EmploymentCareUnitGroupID],
		CAST([HSAID] AS VARCHAR(MAX)) AS [HSAID],
		CAST([HSAIDFullLength] AS VARCHAR(MAX)) AS [HSAIDFullLength],
		CAST([HSAIDTypeID] AS VARCHAR(MAX)) AS [HSAIDTypeID],
		CAST([HasUserBillingPrivs] AS VARCHAR(MAX)) AS [HasUserBillingPrivs],
		CAST([HasUserCareUnitGroupPrivs] AS VARCHAR(MAX)) AS [HasUserCareUnitGroupPrivs],
		CAST([HasUserCareUnitGroupPrivsAdm] AS VARCHAR(MAX)) AS [HasUserCareUnitGroupPrivsAdm],
		CAST([HasUserCareUnitPrivs] AS VARCHAR(MAX)) AS [HasUserCareUnitPrivs],
		CAST([HasUserProfiles] AS VARCHAR(MAX)) AS [HasUserProfiles],
		CONVERT(varchar(max), [InactivationDate], 126) AS [InactivationDate],
		CAST([IsSystem] AS VARCHAR(MAX)) AS [IsSystem],
		CONVERT(varchar(max), [LastLoginDateTime], 126) AS [LastLoginDateTime],
		CONVERT(varchar(max), [LastPasswordChangeDate], 126) AS [LastPasswordChangeDate],
		CONVERT(varchar(max), [LastReceivedTicketDateTime], 126) AS [LastReceivedTicketDateTime],
		CAST([LoginType] AS VARCHAR(MAX)) AS [LoginType],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([ProfessionID] AS VARCHAR(MAX)) AS [ProfessionID],
		CAST([SavedByName] AS VARCHAR(MAX)) AS [SavedByName],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([UserID] AS VARCHAR(MAX)) AS [UserID],
		CAST([UserName] AS VARCHAR(MAX)) AS [UserName],
		CONVERT(varchar(max), [ValidFromDate], 126) AS [ValidFromDate],
		CONVERT(varchar(max), [ValidThroughDate], 126) AS [ValidThroughDate],
		CAST([Version] AS VARCHAR(MAX)) AS [Version],
		CAST([VersionUserID] AS VARCHAR(MAX)) AS [VersionUserID] 
	FROM Intelligence.viewreader.vUsers) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    