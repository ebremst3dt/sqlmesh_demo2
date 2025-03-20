
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Intygstyper för Webcert",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Description': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidFromDate': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'WebcertCertificateTypeCode': 'varchar(max)', 'WebcertCertificateTypeName': 'varchar(max)'},
    column_descriptions={'WebcertCertificateTypeCode': "{'title_ui': None, 'description': 'Extern kod för intygstyp som används för Webcert'}", 'WebcertCertificateTypeName': "{'title_ui': 'Typ av intyg', 'description': 'Namnet på intygstypen som används för Webcert. Visas för användaren när den väljer att skapa ett intyg.'}", 'Description': "{'title_ui': 'Förklaring', 'description': 'Förklarande text för intygstypen som används för Webcert.'}", 'ValidThroughDate': "{'title_ui': None, 'description': 'Giltig t.o.m.'}", 'ValidFromDate': "{'title_ui': None, 'description': 'Giltig fr.o.m.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidFromDate, 126) AS ValidFromDate,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate,
		CAST(WebcertCertificateTypeCode AS VARCHAR(MAX)) AS WebcertCertificateTypeCode,
		CAST(WebcertCertificateTypeName AS VARCHAR(MAX)) AS WebcertCertificateTypeName 
	FROM Intelligence.viewreader.vCodes_WebcertCertificateTypes) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    