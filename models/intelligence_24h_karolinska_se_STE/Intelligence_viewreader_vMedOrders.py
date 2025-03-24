
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Läkemedelsordinationer",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ChangeReasonID': 'varchar(max)', 'CreatedAtCareUnitID': 'varchar(max)', 'DatabaseID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DosageType': 'varchar(max)', 'ExternalPrescriber': 'varchar(max)', 'ExternalStartDate': 'varchar(max)', 'HasOrdinationReason': 'varchar(max)', 'IsMixture': 'varchar(max)', 'IsTriggeredByATC': 'varchar(max)', 'ParentDocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProfylaxID': 'varchar(max)', 'RegistrationStatus': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'SignedByUserID': 'varchar(max)', 'SignedDatetime': 'varchar(max)', 'SignerUserID': 'varchar(max)', 'TimestampCreated': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': 'Version', 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'ParentDocumentID': "{'title_ui': None, 'description': 'Skapad från denna ordination'}", 'TimestampSaved': "{'title_ui': 'Version skapad', 'description': 'Tidpunkt då denna version sparades'}", 'TimestampCreated': "{'title_ui': None, 'description': 'Tidpunkt då ordinationen skapades. Ny 2008-08.'}", 'SavedByUserID': "{'title_ui': 'Version skapad av', 'description': 'Den användare som senast har ändrat dokumentet'}", 'SignerUserID': "{'title_ui': 'Ordinatör', 'description': 'Tillika signeringsansvarig.'}", 'SignedByUserID': "{'title_ui': 'Signerat av', 'description': 'Användaren som signerat ordinationen'}", 'SignedDatetime': "{'title_ui': 'Signerad', 'description': 'Tidpunkt för signering'}", 'RegistrationStatus': "{'title_ui': None, 'description': '-2 = makulerat (innebär att ordinationen aldrig administrerats)'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Vårdenheten där denna version är sparad'}", 'CreatedAtCareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'DatabaseID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'IsMixture': "{'title_ui': 'Blandning/spädning', 'description': 'Om ordinationen gäller en blandning eller spädning av flera läkemedel (inte samma sak som stamlösning)'}", 'DosageType': "{'title_ui': 'Ordinationstyp', 'description': {'break': [None, None, None, None]}}", 'ExternalStartDate': "{'title_ui': 'Insatt datum (ungefärligt)', 'description': 'Om läkemedel satts in utanför läkemedelsjournalen kan fritextdatum anges här'}", 'ExternalPrescriber': "{'title_ui': 'Extern ordinatör (namn)', 'description': 'Den person som satt in läkemedel externt'}", 'ChangeReasonID': "{'title_ui': 'Ändrings-/makuleringsorsak', 'description': 'Orsak till varför ordinationen ändrades eller makulerades senast'}", 'HasOrdinationReason': "{'title_ui': None, 'description': 'Om ordinationsorsak finns'}", 'IsTriggeredByATC': "{'title_ui': None, 'description': 'Om ordination triggad av infektionsverktyget'}", 'ProfylaxID': "{'title_ui': 'Ordinationsorsak', 'description': 'Det profylax som används. 1=Annat, 2=Peroperativ.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ChangeReasonID] AS VARCHAR(MAX)) AS [ChangeReasonID],
		CAST([CreatedAtCareUnitID] AS VARCHAR(MAX)) AS [CreatedAtCareUnitID],
		CAST([DatabaseID] AS VARCHAR(MAX)) AS [DatabaseID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([DosageType] AS VARCHAR(MAX)) AS [DosageType],
		CAST([ExternalPrescriber] AS VARCHAR(MAX)) AS [ExternalPrescriber],
		CAST([ExternalStartDate] AS VARCHAR(MAX)) AS [ExternalStartDate],
		CAST([HasOrdinationReason] AS VARCHAR(MAX)) AS [HasOrdinationReason],
		CAST([IsMixture] AS VARCHAR(MAX)) AS [IsMixture],
		CAST([IsTriggeredByATC] AS VARCHAR(MAX)) AS [IsTriggeredByATC],
		CAST([ParentDocumentID] AS VARCHAR(MAX)) AS [ParentDocumentID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ProfylaxID] AS VARCHAR(MAX)) AS [ProfylaxID],
		CAST([RegistrationStatus] AS VARCHAR(MAX)) AS [RegistrationStatus],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([SignedByUserID] AS VARCHAR(MAX)) AS [SignedByUserID],
		CONVERT(varchar(max), [SignedDatetime], 126) AS [SignedDatetime],
		CAST([SignerUserID] AS VARCHAR(MAX)) AS [SignerUserID],
		CONVERT(varchar(max), [TimestampCreated], 126) AS [TimestampCreated],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMedOrders) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    