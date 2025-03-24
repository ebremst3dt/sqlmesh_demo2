
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Används för att matcha beställda analyser mot besvarade. Innehåller ofta samma data som Analyses men inte alltid",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'Dicipline': 'varchar(max)', 'DocumentID': 'varchar(max)', 'HasReply': 'varchar(max)', 'OrderableID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Specimen': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'OrderableID': "{'title_ui': None, 'description': 'Kod för vald beställningsspec. Dvs. beställningsbar kombination av analys, undersökning, rör och provmaterial'}", 'Analysis': "{'title_ui': 'Ej besvarade analyser', 'description': 'Vald analys/undersökning i klartext'}", 'Specimen': "{'title_ui': 'Ej besvarade analyser', 'description': 'Provmaterial'}", 'Dicipline': "{'title_ui': None, 'description': {'break': [None, None, None, None, None, None, None, None, None, None, None, None, None]}}", 'HasReply': '{\'title_ui\': None, \'description\': \'Har besvarats och visas inte längre i listan "Ej besvarade analyser"\'}', 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([Analysis] AS VARCHAR(MAX)) AS [Analysis],
		CAST([Dicipline] AS VARCHAR(MAX)) AS [Dicipline],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([HasReply] AS VARCHAR(MAX)) AS [HasReply],
		CAST([OrderableID] AS VARCHAR(MAX)) AS [OrderableID],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([Specimen] AS VARCHAR(MAX)) AS [Specimen],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMultiLabOrders_AnalysisMatching) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    