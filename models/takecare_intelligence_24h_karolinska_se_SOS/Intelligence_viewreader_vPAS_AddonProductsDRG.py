
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Produkter som angetts i DRG-formuläret under tilläggsdebiteringar",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AddonProductID': 'varchar(max)', 'Amount': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Price': 'varchar(max)', 'ProductCode': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Weight': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'AddonProductID': "{'title_ui': None, 'description': 'Löpnummer'}", 'ProductCode': "{'title_ui': 'Kod', 'description': 'Produktkod (tilläggsdebiteringskod)'}", 'Price': "{'title_ui': 'A-Pris', 'description': 'Produktens viktade pris'}", 'Weight': "{'title_ui': None, 'description': 'Produktvikt'}", 'Amount': "{'title_ui': 'Antal', 'description': 'Antal av denna produkt'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AddonProductID AS VARCHAR(MAX)) AS AddonProductID,
		CAST(Amount AS VARCHAR(MAX)) AS Amount,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Price AS VARCHAR(MAX)) AS Price,
		CAST(ProductCode AS VARCHAR(MAX)) AS ProductCode,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Weight AS VARCHAR(MAX)) AS Weight 
	FROM Intelligence.viewreader.vPAS_AddonProductsDRG) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    