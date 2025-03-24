
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Registrering/korrigering av medicinska uppgifter ( Åtgärder/Operationer) för slutenvården. Varje åtgärd hör ihop med en diagnos.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ActionCode': 'varchar(max)', 'ActionDate': 'varchar(max)', 'DiagnosisRow': 'varchar(max)', 'FileName': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'DiagnosisRow': "{'title_ui': None, 'description': 'Pekar på den diagnos till vilken åtgärden hör'}", 'Row': "{'title_ui': None, 'description': 'Inmatningsordning för operationen. Den första är huvudoperation.'}", 'ActionCode': "{'title_ui': None, 'description': 'Åtgärdskod (operationskod)'}", 'ActionDate': "{'title_ui': None, 'description': 'Datum för åtgärd'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([ActionCode] AS VARCHAR(MAX)) AS [ActionCode],
		CONVERT(varchar(max), [ActionDate], 126) AS [ActionDate],
		CAST([DiagnosisRow] AS VARCHAR(MAX)) AS [DiagnosisRow],
		CAST([FileName] AS VARCHAR(MAX)) AS [FileName],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransactionID] AS VARCHAR(MAX)) AS [TransactionID] 
	FROM Intelligence.viewreader.vGVR_InpatientActions) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    