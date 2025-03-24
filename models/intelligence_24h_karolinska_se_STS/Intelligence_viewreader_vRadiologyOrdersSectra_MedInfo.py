
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kompletterande uppgifter (tidigare medicinsk information).",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Answer': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsRequired': 'varchar(max)', 'MedInfoCode': 'varchar(max)', 'MedInfoID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'TextLabel': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)', 'Unit': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'MedInfoID': "{'title_ui': None, 'description': 'Identifierar en ledtext. Kompletterad med kolumn MedInfoCode som även hanterar icke-numeriska värden.'}", 'MedInfoCode': "{'title_ui': None, 'description': 'Identifierar en ledtext. Kompletterar kolumn MedInfoID genom att spara även icke-numeriska strängvärden.'}", 'TextLabel': "{'title_ui': None, 'description': 'Ledtext'}", 'Type': "{'title_ui': None, 'description': 'Anger vilken typ av svar som kan matas in för denna kompl. uppgift'}", 'Unit': "{'title_ui': None, 'description': 'Enhet'}", 'IsRequired': "{'title_ui': None, 'description': 'Obligatorisk'}", 'Answer': "{'title_ui': None, 'description': 'Svar'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Answer] AS VARCHAR(MAX)) AS [Answer],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([IsRequired] AS VARCHAR(MAX)) AS [IsRequired],
		CAST([MedInfoCode] AS VARCHAR(MAX)) AS [MedInfoCode],
		CAST([MedInfoID] AS VARCHAR(MAX)) AS [MedInfoID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([TextLabel] AS VARCHAR(MAX)) AS [TextLabel],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Type] AS VARCHAR(MAX)) AS [Type],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vRadiologyOrdersSectra_MedInfo) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    