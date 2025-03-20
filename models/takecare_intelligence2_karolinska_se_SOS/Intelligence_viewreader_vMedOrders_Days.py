
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Ordinationsdata för specifika dagar. Tillsammans med doseringsdata får man alla administreringsuppgifter för en specifik dag. Denna data gäller varje dag, hela dagarna, från och med angivet datum.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AdministrationStartDate': 'varchar(max)', 'AdministrationStartTime': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DosageInstruction': 'varchar(max)', 'DosageInstructionTemplate': 'varchar(max)', 'InfusionTime': 'varchar(max)', 'IsSelfAdministered': 'varchar(max)', 'MaxDailyDose': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': 'Version', 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Användaren som sparat data'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Där data är sparat'}", 'SignerUserID': "{'title_ui': 'Signeringsansvarig', 'description': 'Användaren som är ansvarig för att signera data'}", 'AdministrationStartDate': "{'title_ui': 'Administrationsdatum', 'description': 'Det datum denna data gäller från och med'}", 'AdministrationStartTime': "{'title_ui': 'Administrationstid', 'description': 'Den tid denna data gäller från och med'}", 'MaxDailyDose': "{'title_ui': 'Max dos/dygn', 'description': 'Den maximala dygnsdosen. Används bl.a. för vid-behov-doseringar.'}", 'InfusionTime': "{'title_ui': 'Infusionstid', 'description': 'Hur lång infusionstid i minuter'}", 'IsSelfAdministered': "{'title_ui': 'Pat sköter själv adm', 'description': 'Om patienten sköter administrering själv. Då registreras ingen administrering av personalen.'}", 'DosageInstruction': "{'title_ui': 'Doseringsanvisning för recept', 'description': 'Ny 2006-12'}", 'DosageInstructionTemplate': "{'title_ui': 'Typ av anvisning', 'description': {'break': [None, None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), AdministrationStartDate, 126) AS AdministrationStartDate,
		CONVERT(varchar(max), AdministrationStartTime, 126) AS AdministrationStartTime,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(DosageInstruction AS VARCHAR(MAX)) AS DosageInstruction,
		CAST(DosageInstructionTemplate AS VARCHAR(MAX)) AS DosageInstructionTemplate,
		CAST(InfusionTime AS VARCHAR(MAX)) AS InfusionTime,
		CAST(IsSelfAdministered AS VARCHAR(MAX)) AS IsSelfAdministered,
		CAST(MaxDailyDose AS VARCHAR(MAX)) AS MaxDailyDose,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CAST(SignerUserID AS VARCHAR(MAX)) AS SignerUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMedOrders_Days) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    