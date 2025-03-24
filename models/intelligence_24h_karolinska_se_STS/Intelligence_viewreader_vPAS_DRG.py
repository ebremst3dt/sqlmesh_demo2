
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Uppgifter som registreras i DRG-formuläret i TakeCare",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CarePlanDocumentID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'HasAdditionalMeasures': 'varchar(max)', 'ICUDays': 'varchar(max)', 'ICUPoints': 'varchar(max)', 'IsAutopsied': 'varchar(max)', 'KatzID': 'varchar(max)', 'OutlierLimit': 'varchar(max)', 'PatientID': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductName': 'varchar(max)', 'ProductType': 'varchar(max)', 'ProductWeight': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'WeightAtBirth': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'ProductType': "{'title_ui': None, 'description': 'Typ av DRG-grupperare, ej produkttyp'}", 'ProductCode': "{'title_ui': 'DRG-kod', 'description': 'Produkt. Från DRG-grupperaren.'}", 'ProductName': "{'title_ui': 'DRG-text', 'description': 'Produktens namn/beskrivning. Från DRG-grupperaren.'}", 'ProductWeight': "{'title_ui': 'DRG-vikt', 'description': 'Antal poäng för angiven behandling. Från DRG-grupperaren.'}", 'OutlierLimit': "{'title_ui': 'Ytterfallsgräns', 'description': 'På förhand uppskattad längd på vårdförloppet i dagar. Från DRG-grupperaren.'}", 'KatzID': "{'title_ui': 'KATZ-kod(in)', 'description': 'Anges i DRG-formuläret under vissa förutsättningar, ex. på geriatriken'}", 'HasAdditionalMeasures': "{'title_ui': 'Särskild insats', 'description': 'Anges i DRG-formuläret under vissa förutsättningar, ex. på geriatriken. Kallas också SINS.'}", 'WeightAtBirth': "{'title_ui': 'Födelsevikt', 'description': 'Anges i DRG-formuläret under vissa förutsättningar'}", 'ICUPoints': "{'title_ui': 'IVA-poäng', 'description': 'Anges i DRG-formuläret under vissa förutsättningar'}", 'ICUDays': "{'title_ui': 'IVA-dagar/IVA-statistikdagar', 'description': 'Antingen IVA-dagar (för vuxna) eller IVA-ytterfallsdagar (för barn).'}", 'IsAutopsied': "{'title_ui': 'Obducerad', 'description': 'Om patienten har obducerats'}", 'CarePlanDocumentID': "{'title_ui': 'Kopplad vårdplanering', 'description': 'Dokument-id för kopplad vårdplan (från DRG-formuläret)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CarePlanDocumentID] AS VARCHAR(MAX)) AS [CarePlanDocumentID],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([HasAdditionalMeasures] AS VARCHAR(MAX)) AS [HasAdditionalMeasures],
		CAST([ICUDays] AS VARCHAR(MAX)) AS [ICUDays],
		CAST([ICUPoints] AS VARCHAR(MAX)) AS [ICUPoints],
		CAST([IsAutopsied] AS VARCHAR(MAX)) AS [IsAutopsied],
		CAST([KatzID] AS VARCHAR(MAX)) AS [KatzID],
		CAST([OutlierLimit] AS VARCHAR(MAX)) AS [OutlierLimit],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([ProductCode] AS VARCHAR(MAX)) AS [ProductCode],
		CAST([ProductName] AS VARCHAR(MAX)) AS [ProductName],
		CAST([ProductType] AS VARCHAR(MAX)) AS [ProductType],
		CAST([ProductWeight] AS VARCHAR(MAX)) AS [ProductWeight],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([WeightAtBirth] AS VARCHAR(MAX)) AS [WeightAtBirth] 
	FROM Intelligence.viewreader.vPAS_DRG) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    