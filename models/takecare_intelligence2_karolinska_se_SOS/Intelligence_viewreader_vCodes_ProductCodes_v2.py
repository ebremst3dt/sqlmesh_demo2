
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Produkter för DRG. Kommer ursprungligen från kodservern. Innehåller alla versioner. För produkttyp A saknas giltigt t.o.m. datum, Intelligence lagrar därför det konstruerade datumet 2099-09-09 i ValidThroughDate då denna kolumn ingår i primärnyckeln. Produkttyp E kan innehålla rader där ProductType, ProductCode och ValidThroughDate är samma men där övriga kolumner skiljer sig. Kolumnen Row har därför lagts till.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Description': 'varchar(max)', 'Name': 'varchar(max)', 'OutlierLimit': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductType': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)', 'Weight': 'varchar(max)'},
    column_descriptions={'ProductType': "{'title_ui': None, 'description': None}", 'ProductCode': "{'title_ui': None, 'description': None}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ValidThroughDate': "{'title_ui': None, 'description': None}", 'Name': "{'title_ui': None, 'description': None}", 'Description': "{'title_ui': None, 'description': None}", 'Weight': "{'title_ui': None, 'description': None}", 'OutlierLimit': "{'title_ui': None, 'description': 'Ytterfallsgräns'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Description AS VARCHAR(MAX)) AS Description,
		CAST(Name AS VARCHAR(MAX)) AS Name,
		CAST(OutlierLimit AS VARCHAR(MAX)) AS OutlierLimit,
		CAST(ProductCode AS VARCHAR(MAX)) AS ProductCode,
		CAST(ProductType AS VARCHAR(MAX)) AS ProductType,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate,
		CAST(Weight AS VARCHAR(MAX)) AS Weight 
	FROM Intelligence.viewreader.vCodes_ProductCodes_v2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    