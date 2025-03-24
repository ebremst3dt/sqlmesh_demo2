
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="En journaltextmall används som utgångspunkt för en journalanteckning (samma för alla versioner). Tilläggsmallar kan användas för att utöka den första mallen med fler grupperade sökord. Mallarnas id:n är unika per vårdenhet, oavsett typ. Systemgemensamma mallars id:n är dock globalt unika. Mallinformationen finns endast för journalanteckningar skrivna från och med sommaren 2006, tidigare sparades endast mallnamnet och inte mall-ID.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsAdditionalTemplate': 'varchar(max)', 'PatientID': 'varchar(max)', 'TemplateID': 'varchar(max)', 'TemplateName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Är alltid 1 för att möjliggöra främmande nyckel till huvudtabellen.'}", 'CareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet varifrån mallen hämtats. Null innebär en systemgemensam tilläggsmall.'}", 'TemplateID': "{'title_ui': None, 'description': 'Id för journaltextmallen'}", 'TemplateName': "{'title_ui': 'Mallnamn', 'description': 'Namn på journaltextmallen (saknas för tilläggsmallar)'}", 'IsAdditionalTemplate': "{'title_ui': 'Är en tilläggsmall', 'description': 'Om mallen är en tilläggsmall (annars är den en vanlig mall)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([IsAdditionalTemplate] AS VARCHAR(MAX)) AS [IsAdditionalTemplate],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([TemplateID] AS VARCHAR(MAX)) AS [TemplateID],
		CAST([TemplateName] AS VARCHAR(MAX)) AS [TemplateName],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vCaseNotes_Templates) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    