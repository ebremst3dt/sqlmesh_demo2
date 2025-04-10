
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read

    
@model(
    description="Katalogen för kompletterande uppgifter (tidigare medicinsk information) för FlexLab-analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MedInfoID': 'varchar(max)', 'TextLabel': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)', 'Unit': 'varchar(max)'},
    column_descriptions={'MedInfoID': "{'title_ui': None, 'description': 'Kompl. uppgift id'}", 'TextLabel': "{'title_ui': None, 'description': 'Ledtext/Meddelande'}", 'Type': "{'title_ui': None, 'description': 'Typ ex NoReply eller Numeric'}", 'Unit': "{'title_ui': None, 'description': 'Enhet Om Type=Numeric annars tom'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([MedInfoID] AS VARCHAR(MAX)) AS [MedInfoID],
		CAST([TextLabel] AS VARCHAR(MAX)) AS [TextLabel],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Type] AS VARCHAR(MAX)) AS [Type],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit] 
	FROM Intelligence.viewreader.vCodes_CLFlexLabMedInfo) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    