
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    table_description="Mallar för brev och kallelser. Ej versionshanterade. Innehållet i en mall kan ändras.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CreatedAtCareUnitID': 'varchar(max)', 'TemplateID': 'varchar(max)', 'TemplateName': 'varchar(max)', 'TemplateTypeID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'CreatedAtCareUnitID': "{'title_ui': 'Tillhör vårdenhet', 'description': 'Vårdenhets-id. Mallen tillhör denna vårdenhet.'}", 'TemplateID': "{'title_ui': None, 'description': 'Mall-id. Komponentnumret i vårdenhetens brevmallsfil där mallen finns.'}", 'TemplateName': "{'title_ui': 'Mallnamn', 'description': None}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': None}", 'TemplateTypeID': "{'title_ui': 'Typ/Typ av mall', 'description': {'break': [None, None]}}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CreatedAtCareUnitID AS VARCHAR(MAX)) AS CreatedAtCareUnitID,
		CAST(TemplateID AS VARCHAR(MAX)) AS TemplateID,
		CAST(TemplateName AS VARCHAR(MAX)) AS TemplateName,
		CAST(TemplateTypeID AS VARCHAR(MAX)) AS TemplateTypeID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_LetterTemplates) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    