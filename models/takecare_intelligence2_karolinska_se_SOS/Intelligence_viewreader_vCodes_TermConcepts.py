
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Begreppstillhörighet. Ett sökord kan ha begrepp kopplade till sig, som ett slags semantik.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'ConceptTermID': 'varchar(max)', 'TermID': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'TermID': "{'title_ui': 'Term id', 'description': 'Termens id'}", 'ConceptTermID': "{'title_ui': None, 'description': 'Begreppstermens term-id'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(ConceptTermID AS VARCHAR(MAX)) AS ConceptTermID,
		CAST(TermID AS VARCHAR(MAX)) AS TermID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_TermConcepts) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    