
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Valda analyser. Skapas av det gränssnitt som används på Karolinska Universitetssjukhuset.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Analysis': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'Batch': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Examination': 'varchar(max)', 'ExaminationID': 'varchar(max)', 'IsAllRequired': 'varchar(max)', 'IsRequired': 'varchar(max)', 'Localization': 'varchar(max)', 'PatientID': 'varchar(max)', 'Row': 'varchar(max)', 'ServiceGroup': 'varchar(max)', 'Specimen': 'varchar(max)', 'SpecimenID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'SpecimenID': "{'title_ui': None, 'description': 'Kod för valt provmaterial'}", 'Specimen': "{'title_ui': 'Provmaterial', 'description': None}", 'ExaminationID': "{'title_ui': None, 'description': 'Kod för vald undersökning'}", 'Examination': "{'title_ui': None, 'description': 'Vald undersökning i klartext'}", 'AnalysisID': "{'title_ui': None, 'description': 'Kod för vald analys'}", 'Analysis': "{'title_ui': 'Analys', 'description': 'Vald analys i klartext'}", 'Localization': "{'title_ui': 'Lokalisation', 'description': 'Var på kroppen som provet tagits.'}", 'ServiceGroup': "{'title_ui': None, 'description': 'Tjänstegrupp'}", 'IsAllRequired': "{'title_ui': None, 'description': 'Samtliga analyser är obligatoriska inom paketet'}", 'IsRequired': "{'title_ui': None, 'description': 'Analysen är obligatorisk inom paketet'}", 'Batch': "{'title_ui': 'Paket', 'description': None}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Analysis AS VARCHAR(MAX)) AS Analysis,
		CAST(AnalysisID AS VARCHAR(MAX)) AS AnalysisID,
		CAST(Batch AS VARCHAR(MAX)) AS Batch,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Examination AS VARCHAR(MAX)) AS Examination,
		CAST(ExaminationID AS VARCHAR(MAX)) AS ExaminationID,
		CAST(IsAllRequired AS VARCHAR(MAX)) AS IsAllRequired,
		CAST(IsRequired AS VARCHAR(MAX)) AS IsRequired,
		CAST(Localization AS VARCHAR(MAX)) AS Localization,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(ServiceGroup AS VARCHAR(MAX)) AS ServiceGroup,
		CAST(Specimen AS VARCHAR(MAX)) AS Specimen,
		CAST(SpecimenID AS VARCHAR(MAX)) AS SpecimenID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vBactLabOrders_Analyses2) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    