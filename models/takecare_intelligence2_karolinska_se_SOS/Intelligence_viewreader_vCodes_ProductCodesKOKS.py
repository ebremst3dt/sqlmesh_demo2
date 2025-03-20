
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Produkter för KÖKS. Kommer ursprungligen från kodservern.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'IsKOKSCode': 'varchar(max)', 'KOKSCode': 'varchar(max)', 'KOKSName': 'varchar(max)', 'ProductCode': 'varchar(max)', 'ProductName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Weight': 'varchar(max)'},
    column_descriptions={'KOKSCode': "{'title_ui': None, 'description': None}", 'KOKSName': "{'title_ui': None, 'description': 'Beteckning'}", 'ProductCode': "{'title_ui': None, 'description': 'Produkt (typ och kod), skickas till GVR'}", 'ProductName': "{'title_ui': None, 'description': 'Produktens beteckning'}", 'Weight': "{'title_ui': None, 'description': 'Vikt'}", 'IsKOKSCode': "{'title_ui': None, 'description': '1 om KÖKS-kod, annars produktkod (om 0 ingen KÖKS-kod till GVR)'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(IsKOKSCode AS VARCHAR(MAX)) AS IsKOKSCode,
		CAST(KOKSCode AS VARCHAR(MAX)) AS KOKSCode,
		CAST(KOKSName AS VARCHAR(MAX)) AS KOKSName,
		CAST(ProductCode AS VARCHAR(MAX)) AS ProductCode,
		CAST(ProductName AS VARCHAR(MAX)) AS ProductName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Weight AS VARCHAR(MAX)) AS Weight 
	FROM Intelligence.viewreader.vCodes_ProductCodesKOKS) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    