
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.takecare import start

    
@model(
    description="Apotekets/SILs läkemedelsregister. Samma preparat upprepas för varje styrka och läkemedelsform.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ATC': 'varchar(max)', 'Agent': 'varchar(max)', 'AgentID': 'varchar(max)', 'ArticleStrength': 'varchar(max)', 'AssortmentCode': 'varchar(max)', 'ControlDrugCode': 'varchar(max)', 'DDDPerPack': 'varchar(max)', 'DatabaseID': 'varchar(max)', 'Description': 'varchar(max)', 'DoseForm': 'varchar(max)', 'DoseFormCode': 'varchar(max)', 'DrugID': 'varchar(max)', 'DrugName': 'varchar(max)', 'DrugNo': 'varchar(max)', 'ExpirationDate': 'varchar(max)', 'IsCostFree': 'varchar(max)', 'IsOnPrescription': 'varchar(max)', 'IsPriceReducted': 'varchar(max)', 'Name': 'varchar(max)', 'PackageDescription': 'varchar(max)', 'PackageSize': 'varchar(max)', 'PreparationID': 'varchar(max)', 'Price': 'varchar(max)', 'ProducerID': 'varchar(max)', 'ProducerName': 'varchar(max)', 'PurchasePrice': 'varchar(max)', 'RegistryNo': 'varchar(max)', 'SpecialityID': 'varchar(max)', 'Strength': 'varchar(max)', 'StrengthUnit': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UnitCodeID': 'varchar(max)', 'UnitCodeText': 'varchar(max)'},
    column_descriptions={'DrugID': "{'title_ui': 'Varu-Id', 'description': 'Varu-id/förpacknings-id'}", 'DrugNo': "{'title_ui': 'Nordiskt varunummer', 'description': None}", 'SpecialityID': "{'title_ui': 'Specid', 'description': 'Preparatets specialitets-id/nplId (Apotekets/SILs interna ID)'}", 'PreparationID': "{'title_ui': 'Preparat-id', 'description': 'Preparatets preparat-id/drugId (Apotekets/SILs interna ID)'}", 'RegistryNo': "{'title_ui': 'Reg.nummer', 'description': 'Läkemedelsverkets registreringsnummer'}", 'Name': "{'title_ui': 'Preparatnamn', 'description': 'Preparatets namn'}", 'ATC': "{'title_ui': 'ATC-kod', 'description': 'Preparatets ATC-kod'}", 'Strength': "{'title_ui': 'Styrka', 'description': 'Preparatets styrka'}", 'StrengthUnit': "{'title_ui': 'Styrkeenhet', 'description': 'Preparatets styrkeenhet'}", 'DatabaseID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'Agent': "{'title_ui': None, 'description': 'Ombud för preparatet'}", 'DoseForm': "{'title_ui': 'Läkemedelsform', 'description': 'Hette tidigare Beredningsform'}", 'IsOnPrescription': "{'title_ui': None, 'description': 'Om preparatet är receptbelagt'}", 'Price': "{'title_ui': 'Utförsäljningspris', 'description': 'Apotekets/SILs utförsäljningspris'}", 'DDDPerPack': "{'title_ui': 'Dygnsdos/förp', 'description': 'Antal dygnsdoser per förpackning'}", 'ExpirationDate': "{'title_ui': None, 'description': 'Datum då preparatet utgår ur Apotekets/SILs varuregister'}", 'PackageSize': "{'title_ui': None, 'description': 'Förpackningsstorlek'}", 'DrugName': "{'title_ui': 'Läkemedelsnamn', 'description': 'Unika läkemedelsnamn'}", 'DoseFormCode': "{'title_ui': 'Läkemedelsform', 'description': 'Läkemedelsform Kod/Saknar värde i SIL'}", 'UnitCodeID': "{'title_ui': 'Enhetskod', 'description': 'Enhetskod'}", 'UnitCodeText': "{'title_ui': 'Enhetskod', 'description': 'Enhetstext'}", 'IsPriceReducted': "{'title_ui': 'Med förmån/Utan förmån', 'description': 'Rabatterat'}", 'IsCostFree': "{'title_ui': None, 'description': 'Kostnadsfritt'}", 'AssortmentCode': "{'title_ui': None, 'description': 'Sortimentkod'}", 'Description': "{'title_ui': 'Varubeskrivning', 'description': 'Beskrivning'}", 'ControlDrugCode': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None, None]}}", 'PackageDescription': "{'title_ui': None, 'description': 'Beskrivning av förpackningen'}", 'ArticleStrength': "{'title_ui': 'Styrkor', 'description': 'Storlek eller styrka på artiklar'}", 'ProducerID': "{'title_ui': None, 'description': 'Kod för tillverkaren/Saknar värde i SIL'}", 'ProducerName': "{'title_ui': None, 'description': 'Tillverkare'}", 'AgentID': "{'title_ui': None, 'description': 'Saknar värde i SIL'}", 'PurchasePrice': "{'title_ui': None, 'description': 'Apotekets/SILs inköpspris'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=5000,
        unique_key=['DrugID']
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
		CAST([ATC] AS VARCHAR(MAX)) AS [ATC],
		CAST([Agent] AS VARCHAR(MAX)) AS [Agent],
		CAST([AgentID] AS VARCHAR(MAX)) AS [AgentID],
		CAST([ArticleStrength] AS VARCHAR(MAX)) AS [ArticleStrength],
		CAST([AssortmentCode] AS VARCHAR(MAX)) AS [AssortmentCode],
		CAST([ControlDrugCode] AS VARCHAR(MAX)) AS [ControlDrugCode],
		CAST([DDDPerPack] AS VARCHAR(MAX)) AS [DDDPerPack],
		CAST([DatabaseID] AS VARCHAR(MAX)) AS [DatabaseID],
		CAST([Description] AS VARCHAR(MAX)) AS [Description],
		CAST([DoseForm] AS VARCHAR(MAX)) AS [DoseForm],
		CAST([DoseFormCode] AS VARCHAR(MAX)) AS [DoseFormCode],
		CAST([DrugID] AS VARCHAR(MAX)) AS [DrugID],
		CAST([DrugName] AS VARCHAR(MAX)) AS [DrugName],
		CAST([DrugNo] AS VARCHAR(MAX)) AS [DrugNo],
		CONVERT(varchar(max), [ExpirationDate], 126) AS [ExpirationDate],
		CAST([IsCostFree] AS VARCHAR(MAX)) AS [IsCostFree],
		CAST([IsOnPrescription] AS VARCHAR(MAX)) AS [IsOnPrescription],
		CAST([IsPriceReducted] AS VARCHAR(MAX)) AS [IsPriceReducted],
		CAST([Name] AS VARCHAR(MAX)) AS [Name],
		CAST([PackageDescription] AS VARCHAR(MAX)) AS [PackageDescription],
		CAST([PackageSize] AS VARCHAR(MAX)) AS [PackageSize],
		CAST([PreparationID] AS VARCHAR(MAX)) AS [PreparationID],
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
		CAST([UnitCodeText] AS VARCHAR(MAX)) AS [UnitCodeText] 
	FROM Intelligence.viewreader.vCodes_Drugs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    