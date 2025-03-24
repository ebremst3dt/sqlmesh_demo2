
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Objekt som hör till en formulärdefinition",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Caption': 'varchar(max)', 'FormTemplateID': 'varchar(max)', 'HasToBeSplittedByChar': 'varchar(max)', 'ObjectID': 'varchar(max)', 'ObjectName': 'varchar(max)', 'ObjectTypeID': 'varchar(max)', 'RecordCode': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TipText': 'varchar(max)'},
    column_descriptions={'FormTemplateID': "{'title_ui': 'Identitet', 'description': 'Formulärdefinitionens id'}", 'ObjectID': "{'title_ui': None, 'description': 'Objektets numeriska id'}", 'ObjectName': "{'title_ui': 'Unikt objektnamn', 'description': 'Används för att koppla fält till Pdf'}", 'ObjectTypeID': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None, None, None, None, None, None, None, None]}}", 'Caption': "{'title_ui': 'Rubrik', 'description': None}", 'RecordCode': "{'title_ui': 'Hämta senaste värde från', 'description': 'Kod för att hämta data från journal'}", 'TipText': "{'title_ui': 'Hjälptext', 'description': None}", 'TermID': "{'title_ui': 'Termkatalogsid', 'description': None}", 'HasToBeSplittedByChar': "{'title_ui': 'Dela upp per tecken', 'description': 'Om värdet går att ange för ett objekt men inget valts visas det upp som Nej i TakeCare trots att inget giltigt värde lagras'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Caption] AS VARCHAR(MAX)) AS [Caption],
		CAST([FormTemplateID] AS VARCHAR(MAX)) AS [FormTemplateID],
		CAST([HasToBeSplittedByChar] AS VARCHAR(MAX)) AS [HasToBeSplittedByChar],
		CAST([ObjectID] AS VARCHAR(MAX)) AS [ObjectID],
		CAST([ObjectName] AS VARCHAR(MAX)) AS [ObjectName],
		CAST([ObjectTypeID] AS VARCHAR(MAX)) AS [ObjectTypeID],
		CAST([RecordCode] AS VARCHAR(MAX)) AS [RecordCode],
		CAST([TermID] AS VARCHAR(MAX)) AS [TermID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TipText] AS VARCHAR(MAX)) AS [TipText] 
	FROM Intelligence.viewreader.vCodes_FormTemplateObjects) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    