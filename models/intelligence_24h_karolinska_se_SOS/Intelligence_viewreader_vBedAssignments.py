
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Alla förändringar av sängplatser och sängar.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BedDescription': 'varchar(max)', 'BedID': 'varchar(max)', 'BedName': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'Category': 'varchar(max)', 'ChangedDate': 'varchar(max)', 'Comment': 'varchar(max)', 'PatientID': 'varchar(max)', 'SortIndex': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)', 'ValidUntilDate': 'varchar(max)'},
    column_descriptions={'CareUnitID': "{'title_ui': None, 'description': 'Vårdenhet'}", 'TransactionID': "{'title_ui': None, 'description': 'Unik nyckel för denna transaktion och vårdenhet'}", 'BedID': "{'title_ui': None, 'description': 'Vårdplats Id'}", 'ChangedDate': "{'title_ui': None, 'description': 'Datum då förändringen skedde'}", 'BedName': '{\'title_ui\': \'Plats\', \'description\': \'Vårdplatsens namn, t.ex. "12:1"\'}', 'BedDescription': '{\'title_ui\': \'Beskrivning\', \'description\': \'En utförligare beskrivning av vårdplatsen, t.ex "syrgas"\'}', 'ValidUntilDate': "{'title_ui': 'Stängd', 'description': 'Efter detta datum (inklusivt) är vårdplatsen inaktiverad'}", 'SortIndex': "{'title_ui': None, 'description': 'Sorteringsindex. På en viss vårdenhet visas vårdplatser i stigande ordning enligt denna kolumn.'}", 'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'Category': '{\'title_ui\': \'Kategori\', \'description\': \'Fritextfält där man kan ange en kategori för patienten, t.ex. "Blått team" eller "Armbrott".\'}', 'Comment': "{'title_ui': 'Kommentar', 'description': 'Fritextfält där man kan ange en kommentar för patienten.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BedDescription] AS VARCHAR(MAX)) AS [BedDescription],
		CAST([BedID] AS VARCHAR(MAX)) AS [BedID],
		CAST([BedName] AS VARCHAR(MAX)) AS [BedName],
		CAST([CareUnitID] AS VARCHAR(MAX)) AS [CareUnitID],
		CAST([Category] AS VARCHAR(MAX)) AS [Category],
		CONVERT(varchar(max), [ChangedDate], 126) AS [ChangedDate],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([SortIndex] AS VARCHAR(MAX)) AS [SortIndex],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransactionID] AS VARCHAR(MAX)) AS [TransactionID],
		CONVERT(varchar(max), [ValidUntilDate], 126) AS [ValidUntilDate] 
	FROM Intelligence.viewreader.vBedAssignments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    