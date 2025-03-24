
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Kompletterande uppgifter (tidigare medicinsk information) som kan anges eller begäras vid en beställning (Mikrolabb analyskatalog)",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MedInfoID': 'varchar(max)', 'OrderRegistryFileName': 'varchar(max)', 'TextLabel': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Type': 'varchar(max)', 'Unit': 'varchar(max)'},
    column_descriptions={'OrderRegistryFileName': "{'title_ui': None, 'description': 'Namnet på den fil där bl.a. analyskatalogen ligger.'}", 'MedInfoID': "{'title_ui': None, 'description': None}", 'TextLabel': "{'title_ui': None, 'description': 'Ledtext'}", 'Type': "{'title_ui': None, 'description': 'Anger vilken typ av svar som kan matas in för denna kompl. uppgift'}", 'Unit': "{'title_ui': None, 'description': 'Enhet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([OrderRegistryFileName] AS VARCHAR(MAX)) AS [OrderRegistryFileName],
		CAST([TextLabel] AS VARCHAR(MAX)) AS [TextLabel],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Type] AS VARCHAR(MAX)) AS [Type],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit] 
	FROM Intelligence.viewreader.vCodes_MicroMedInfo) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STE")
    