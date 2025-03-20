
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Avslut av vårdperiod samt korrigeringar av avslut",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AddictionDiagnosis': 'varchar(max)', 'DiagnosisGroup': 'varchar(max)', 'FileName': 'varchar(max)', 'PatientCategory': 'varchar(max)', 'PersonalityDisorderDiagnosis': 'varchar(max)', 'ReferredTo': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'DiagnosisGroup': "{'title_ui': None, 'description': 'Slutlig diagnosgrupp. Avser den slutliga bedömningen.'}", 'ReferredTo': "{'title_ui': None, 'description': 'Hänvisad till. Används ej.'}", 'PatientCategory': "{'title_ui': None, 'description': 'Slutgiltig patientkategori'}", 'AddictionDiagnosis': "{'title_ui': None, 'description': 'Beroendediagnos'}", 'PersonalityDisorderDiagnosis': "{'title_ui': None, 'description': 'Personlighetsstörning. Används ej.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(AddictionDiagnosis AS VARCHAR(MAX)) AS AddictionDiagnosis,
		CAST(DiagnosisGroup AS VARCHAR(MAX)) AS DiagnosisGroup,
		CAST(FileName AS VARCHAR(MAX)) AS FileName,
		CAST(PatientCategory AS VARCHAR(MAX)) AS PatientCategory,
		CAST(PersonalityDisorderDiagnosis AS VARCHAR(MAX)) AS PersonalityDisorderDiagnosis,
		CAST(ReferredTo AS VARCHAR(MAX)) AS ReferredTo,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TransactionID AS VARCHAR(MAX)) AS TransactionID 
	FROM Intelligence.viewreader.vGVR_CarePeriodEndings) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    