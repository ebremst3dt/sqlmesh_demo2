
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda förpackningar i ett recept. Huvudsakligen information från Apotekets/SILs varuregister.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATC': 'varchar(max)', 'Agent': 'varchar(max)', 'AgentID': 'varchar(max)', 'ArticleStrength': 'varchar(max)', 'AssortmentCode': 'varchar(max)', 'CancellationReply': 'varchar(max)', 'CancellationReplyCode': 'varchar(max)', 'CancellationRequestedBy': 'varchar(max)', 'CancellationTimestamp': 'varchar(max)', 'ControlDrugCode': 'varchar(max)', 'DDDPerPack': 'varchar(max)', 'Description': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DoseForm': 'varchar(max)', 'DoseFormCode': 'varchar(max)', 'DrugID': 'varchar(max)', 'DrugName': 'varchar(max)', 'DrugNameAlternative': 'varchar(max)', 'DrugNo': 'varchar(max)', 'ExpirationDate': 'varchar(max)', 'IsCostFree': 'varchar(max)', 'IsOnPrescription': 'varchar(max)', 'IsPriceReducted': 'varchar(max)', 'NoOfPackages': 'varchar(max)', 'PackageDescription': 'varchar(max)', 'PackageSize': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationID': 'varchar(max)', 'PreparationName': 'varchar(max)', 'Price': 'varchar(max)', 'ProducerID': 'varchar(max)', 'ProducerName': 'varchar(max)', 'PurchasePrice': 'varchar(max)', 'RegistryNo': 'varchar(max)', 'SpecialityID': 'varchar(max)', 'Strength': 'varchar(max)', 'StrengthUnit': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UnitCodeID': 'varchar(max)', 'UnitCodeText': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'NoOfPackages': "{'title_ui': 'Antal förp.', 'description': 'Antal valda förpackningar'}", 'DrugID': "{'title_ui': 'Varu-Id', 'description': 'Varu-id/förpacknings-id/nplPackId (Apotekets/SILs interna ID/Saknas för hjälpmedel och livsmedel i SIL).'}", 'DrugNo': "{'title_ui': 'Nordiskt varunummer', 'description': None}", 'SpecialityID': "{'title_ui': 'Specid', 'description': 'Preparatets specialitets-id/nplId (Apotekets/SILs interna ID/-303 för hjälpmedel och livsmedel från SIL)'}", 'PreparationID': "{'title_ui': 'Preparat-id', 'description': 'Preparatets preparat-id/drugId (Apotekets/SILs interna ID/Saknas för hjälpmedel och livsmedel i SIL)'}", 'RegistryNo': "{'title_ui': 'Reg.nummer', 'description': 'Läkemedelsverkets registreringsnummer. Saknas för hjälpmedel och livsmedel i SIL.'}", 'ATC': "{'title_ui': 'ATC-kod', 'description': 'Preparatets ATC-kod'}", 'PackageSize': "{'title_ui': 'Antal förp.', 'description': 'Förpackningsstorlek'}", 'PreparationName': "{'title_ui': 'Preparatnamn', 'description': 'Preparatets namn'}", 'DrugName': "{'title_ui': 'Läkemedelsnamn', 'description': 'Unika läkemedelsnamn'}", 'DrugNameAlternative': "{'title_ui': 'Läkemedelsnamn(Alternativt)', 'description': None}", 'DoseFormCode': "{'title_ui': 'Läkemedelsform', 'description': 'Läkemedelsform Kod(saknas i SIL)'}", 'DoseForm': "{'title_ui': 'Läkemedelsform', 'description': None}", 'DDDPerPack': "{'title_ui': 'Dygnsdos/förp', 'description': 'Antal dygnsdoser per förpackning'}", 'UnitCodeID': "{'title_ui': 'Enhetskod', 'description': 'Enhetskod'}", 'UnitCodeText': "{'title_ui': 'Enhetskod', 'description': 'Enhetstext'}", 'IsPriceReducted': "{'title_ui': 'Med/Utan förmån', 'description': 'Rabatterat'}", 'IsCostFree': "{'title_ui': None, 'description': 'Kostnadsfritt'}", 'AssortmentCode': "{'title_ui': None, 'description': 'Sortimentkod'}", 'Description': "{'title_ui': 'Varubeskrivning', 'description': 'Beskrivning'}", 'Strength': "{'title_ui': 'Numerisk styrka', 'description': 'Preparatets styrka'}", 'StrengthUnit': "{'title_ui': 'Styrkeenhet', 'description': 'Preparatets styrkeenhet'}", 'ControlDrugCode': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None, None]}}", 'PackageDescription': "{'title_ui': None, 'description': 'Beskrivning av förpackningen'}", 'ArticleStrength': "{'title_ui': 'Styrkor', 'description': 'Storlek eller styrka på artiklar'}", 'Price': "{'title_ui': 'Utförsäljningspris', 'description': 'Apotekets/SILs utförsäljningspris'}", 'ProducerID': "{'title_ui': None, 'description': 'Kod för tillverkaren/Saknar värde i SIL'}", 'ProducerName': "{'title_ui': None, 'description': 'Tillverkare'}", 'AgentID': "{'title_ui': None, 'description': 'Kod för ombud/Saknar värde i SIL'}", 'Agent': "{'title_ui': None, 'description': 'Ombud'}", 'IsOnPrescription': "{'title_ui': None, 'description': 'Om preparatet är receptbelagt'}", 'PurchasePrice': "{'title_ui': None, 'description': 'Apotekets/SILs inköpspris'}", 'ExpirationDate': "{'title_ui': None, 'description': 'Datum då preparatet utgår ur Apotekets/SILs varuregister'}", 'CancellationTimestamp': "{'title_ui': None, 'description': 'Tidpunkt då preparatet processades av apoteket'}", 'CancellationReplyCode': "{'title_ui': None, 'description': 'Svarskod från apoteket'}", 'CancellationReply': "{'title_ui': None, 'description': 'Svarstext från apoteket'}", 'CancellationRequestedBy': "{'title_ui': None, 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ATC] AS VARCHAR(MAX)) AS [ATC],
		CAST([Agent] AS VARCHAR(MAX)) AS [Agent],
		CAST([AgentID] AS VARCHAR(MAX)) AS [AgentID],
		CAST([ArticleStrength] AS VARCHAR(MAX)) AS [ArticleStrength],
		CAST([AssortmentCode] AS VARCHAR(MAX)) AS [AssortmentCode],
		CAST([CancellationReply] AS VARCHAR(MAX)) AS [CancellationReply],
		CAST([CancellationReplyCode] AS VARCHAR(MAX)) AS [CancellationReplyCode],
		CAST([CancellationRequestedBy] AS VARCHAR(MAX)) AS [CancellationRequestedBy],
		CONVERT(varchar(max), [CancellationTimestamp], 126) AS [CancellationTimestamp],
		CAST([ControlDrugCode] AS VARCHAR(MAX)) AS [ControlDrugCode],
		CAST([DDDPerPack] AS VARCHAR(MAX)) AS [DDDPerPack],
		CAST([Description] AS VARCHAR(MAX)) AS [Description],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([DoseForm] AS VARCHAR(MAX)) AS [DoseForm],
		CAST([DoseFormCode] AS VARCHAR(MAX)) AS [DoseFormCode],
		CAST([DrugID] AS VARCHAR(MAX)) AS [DrugID],
		CAST([DrugName] AS VARCHAR(MAX)) AS [DrugName],
		CAST([DrugNameAlternative] AS VARCHAR(MAX)) AS [DrugNameAlternative],
		CAST([DrugNo] AS VARCHAR(MAX)) AS [DrugNo],
		CONVERT(varchar(max), [ExpirationDate], 126) AS [ExpirationDate],
		CAST([IsCostFree] AS VARCHAR(MAX)) AS [IsCostFree],
		CAST([IsOnPrescription] AS VARCHAR(MAX)) AS [IsOnPrescription],
		CAST([IsPriceReducted] AS VARCHAR(MAX)) AS [IsPriceReducted],
		CAST([NoOfPackages] AS VARCHAR(MAX)) AS [NoOfPackages],
		CAST([PackageDescription] AS VARCHAR(MAX)) AS [PackageDescription],
		CAST([PackageSize] AS VARCHAR(MAX)) AS [PackageSize],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([PreparationID] AS VARCHAR(MAX)) AS [PreparationID],
		CAST([PreparationName] AS VARCHAR(MAX)) AS [PreparationName],
		CAST([Price] AS VARCHAR(MAX)) AS [Price],
		CAST([ProducerID] AS VARCHAR(MAX)) AS [ProducerID],
		CAST([ProducerName] AS VARCHAR(MAX)) AS [ProducerName],
		CAST([PurchasePrice] AS VARCHAR(MAX)) AS [PurchasePrice],
		CAST([RegistryNo] AS VARCHAR(MAX)) AS [RegistryNo],
		CAST([SpecialityID] AS VARCHAR(MAX)) AS [SpecialityID],
		CAST([Strength] AS VARCHAR(MAX)) AS [Strength],
		CAST([StrengthUnit] AS VARCHAR(MAX)) AS [StrengthUnit],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([UnitCodeID] AS VARCHAR(MAX)) AS [UnitCodeID],
		CAST([UnitCodeText] AS VARCHAR(MAX)) AS [UnitCodeText],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vPrescriptions_ChosenPackages) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    