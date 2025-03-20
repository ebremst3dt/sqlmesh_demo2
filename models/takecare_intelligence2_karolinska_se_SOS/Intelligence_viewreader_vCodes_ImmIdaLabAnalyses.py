
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""Analyser som kan kopplas till en beställning (Immunologi IdaLab analyskatalog)""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AnalysisID': 'varchar(max)', 'AnalysisName': 'varchar(max)', 'AnalysisShortName': 'varchar(max)', 'IsOrderable': 'varchar(max)', 'OrderTypeID': 'varchar(max)', 'PharmacyCode': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'AnalysisID': "{'title_ui': None, 'description': 'Analysid'}", 'AnalysisShortName': "{'title_ui': None, 'description': 'Analysens kortnamn'}", 'AnalysisName': "{'title_ui': None, 'description': 'Analysnamn'}", 'IsOrderable': "{'title_ui': None, 'description': 'Beställningsbar'}", 'PharmacyCode': "{'title_ui': None, 'description': 'Farmaciakod'}", 'OrderTypeID': "{'title_ui': None, 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AnalysisID AS VARCHAR(MAX)) AS AnalysisID,
		CAST(AnalysisName AS VARCHAR(MAX)) AS AnalysisName,
		CAST(AnalysisShortName AS VARCHAR(MAX)) AS AnalysisShortName,
		CAST(IsOrderable AS VARCHAR(MAX)) AS IsOrderable,
		CAST(OrderTypeID AS VARCHAR(MAX)) AS OrderTypeID,
		CAST(PharmacyCode AS VARCHAR(MAX)) AS PharmacyCode,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vCodes_ImmIdaLabAnalyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    