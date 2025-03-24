
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'Batch': 'varchar(max)', 'BatchID': 'varchar(max)', 'DocumentID': 'varchar(max)', 'IsRequired': 'varchar(max)', 'MaterielID': 'varchar(max)', 'OrderableID': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'Specimen': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'OrderableID': "{'title_ui': None, 'description': 'Kod för vald beställningsspec. Dvs. beställningsbar kombination av analys, undersökning, rör och provmaterial'}", 'AnalysisID': "{'title_ui': 'Analyser', 'description': 'Kod för vald analys'}", 'Analysis': "{'title_ui': 'Analyser', 'description': 'Vald analys i klartext'}", 'SpecimenID': "{'title_ui': 'Provmaterial', 'description': 'Kod för valt provmaterial'}", 'Specimen': "{'title_ui': 'Provmaterial', 'description': None}", 'MaterielID': "{'title_ui': 'Materiel', 'description': 'Materielkod'}", 'BatchID': "{'title_ui': 'Paket', 'description': None}", 'Batch': "{'title_ui': 'Paket', 'description': None}", 'IsRequired': "{'title_ui': None, 'description': 'Analysen är obligatorisk inom paketet'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Analysis AS VARCHAR(MAX)) AS Analysis,
		CAST(AnalysisID AS VARCHAR(MAX)) AS AnalysisID,
		CAST(Batch AS VARCHAR(MAX)) AS Batch,
		CAST(BatchID AS VARCHAR(MAX)) AS BatchID,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(IsRequired AS VARCHAR(MAX)) AS IsRequired,
		CAST(MaterielID AS VARCHAR(MAX)) AS MaterielID,
		CAST(OrderableID AS VARCHAR(MAX)) AS OrderableID,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(Specimen AS VARCHAR(MAX)) AS Specimen,
		CAST(SpecimenID AS VARCHAR(MAX)) AS SpecimenID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vImmLabOrdersProsang_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    