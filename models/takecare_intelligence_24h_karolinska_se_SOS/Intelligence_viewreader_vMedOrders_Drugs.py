
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Läkemedel (preparat) som ingår i ordination. Preparaten har antingen id från Apotekets varuregister/SIL, ett internt id för specialpreparat, eller inget id alls (för egna preparat). En rad per läkemedel för denna ordination. Vid blandning eller stamlösning anges flera rader. ATC-koder fås genom att slå i Apotekets varuregister/SIL.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ATC': 'varchar(max)', 'DocumentID': 'varchar(max)', 'DosageUnitID': 'varchar(max)', 'DoseForm': 'varchar(max)', 'DoseFormCode': 'varchar(max)', 'DrugCode': 'varchar(max)', 'DrugID': 'varchar(max)', 'InternalArticleStrength': 'varchar(max)', 'IsApproved': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationText': 'varchar(max)', 'ProductType': 'varchar(max)', 'Row': 'varchar(max)', 'SpecialDrugCode': 'varchar(max)', 'SpecialityID': 'varchar(max)', 'StdSolutionAmount': 'varchar(max)', 'Strength': 'varchar(max)', 'StrengthUnit': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'UnitCode': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': 'Version', 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'PreparationText': "{'title_ui': 'Preparat', 'description': 'Namn på preparatet'}", 'DrugCode': "{'title_ui': None, 'description': 'Id för preparatet (ej att förväxlas med Apotekets preparat-id). Är antingen specialitets-id/nplId, internt id för specialpreparat eller -101 för eget preparat.'}", 'SpecialityID': "{'title_ui': 'Specid', 'description': 'Apotekets specialitets-id/nplId för preparatet'}", 'SpecialDrugCode': "{'title_ui': None, 'description': 'Internt id för specialpreparat'}", 'DrugID': "{'title_ui': 'Varu-Id', 'description': 'Apotekets varu-id för preparatet/nplPackId'}", 'DoseForm': "{'title_ui': 'Läkemedelsform', 'description': 'Hette tidigare Beredningsform'}", 'DoseFormCode': "{'title_ui': None, 'description': 'Apoteketskod för läkemedelsformen (saknas i SIL)'}", 'ATC': "{'title_ui': 'ATC-kod/benämning', 'description': 'ATC-kod för ingående preparat då eget preparat skapas. Frivilligt att fylla i.'}", 'Strength': "{'title_ui': 'Styrka', 'description': 'Styrka på preparatet (numeriskt)'}", 'StrengthUnit': "{'title_ui': 'Styrkeenhet', 'description': 'Enhet på preparatets styrka'}", 'InternalArticleStrength': "{'title_ui': 'Styrka eller förpackning', 'description': 'Används för att beskriva styrkan av preparatet när styrka (kolumn Strength) saknas som unikt identifierar det valda läkemedlet. Ny 2009-06-01'}", 'UnitCode': "{'title_ui': 'Enhetskod', 'description': 'Enhetskod för förpackningen (ex. ML, ST)'}", 'DosageUnitID': "{'title_ui': 'Dosenhet', 'description': None}", 'StdSolutionAmount': "{'title_ui': 'Lösning, mängd', 'description': 'Mängd av preparatet i lösning/stamlösning'}", 'ProductType': "{'title_ui': 'Produkttyp', 'description': {'break': [None, None, None]}}", 'IsApproved': "{'title_ui': 'Ej godkänt', 'description': 'Ej godkänt läkemdel enligt Läkemedelsverket'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(ATC AS VARCHAR(MAX)) AS ATC,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(DosageUnitID AS VARCHAR(MAX)) AS DosageUnitID,
		CAST(DoseForm AS VARCHAR(MAX)) AS DoseForm,
		CAST(DoseFormCode AS VARCHAR(MAX)) AS DoseFormCode,
		CAST(DrugCode AS VARCHAR(MAX)) AS DrugCode,
		CAST(DrugID AS VARCHAR(MAX)) AS DrugID,
		CAST(InternalArticleStrength AS VARCHAR(MAX)) AS InternalArticleStrength,
		CAST(IsApproved AS VARCHAR(MAX)) AS IsApproved,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PreparationText AS VARCHAR(MAX)) AS PreparationText,
		CAST(ProductType AS VARCHAR(MAX)) AS ProductType,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SpecialDrugCode AS VARCHAR(MAX)) AS SpecialDrugCode,
		CAST(SpecialityID AS VARCHAR(MAX)) AS SpecialityID,
		CAST(StdSolutionAmount AS VARCHAR(MAX)) AS StdSolutionAmount,
		CAST(Strength AS VARCHAR(MAX)) AS Strength,
		CAST(StrengthUnit AS VARCHAR(MAX)) AS StrengthUnit,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(UnitCode AS VARCHAR(MAX)) AS UnitCode,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vMedOrders_Drugs) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    