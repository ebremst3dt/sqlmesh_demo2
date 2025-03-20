
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Registrering/korrigering av medicinska uppgifter (Diagnoser) för öppenvården. Om diagnosen har en orsakskod så lagras den som en rad i den här tabellen.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CarePlanningID': 'varchar(max)', 'ClinicalPathwayNumber': 'varchar(max)', 'DiagnosisCode': 'varchar(max)', 'FileName': 'varchar(max)', 'ReferringCareUnit': 'varchar(max)', 'ReferringClinic': 'varchar(max)', 'ReferringHospital': 'varchar(max)', 'Row': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'Row': "{'title_ui': None, 'description': 'Inmatningsordning för diagnos. Den första är huvuddiagnosen.'}", 'ReferringHospital': "{'title_ui': None, 'description': 'Remitterande inrättning'}", 'ReferringClinic': "{'title_ui': None, 'description': 'Remitterande klinik'}", 'ReferringCareUnit': "{'title_ui': None, 'description': 'Remitterande avdelning'}", 'CarePlanningID': "{'title_ui': None, 'description': 'Vårdplaneringskod'}", 'DiagnosisCode': "{'title_ui': None, 'description': 'Diagnos-kod eller ATC-kod eller orsakskod'}", 'ClinicalPathwayNumber': "{'title_ui': None, 'description': 'Vårdkedjenummer. Används ej.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CarePlanningID AS VARCHAR(MAX)) AS CarePlanningID,
		CAST(ClinicalPathwayNumber AS VARCHAR(MAX)) AS ClinicalPathwayNumber,
		CAST(DiagnosisCode AS VARCHAR(MAX)) AS DiagnosisCode,
		CAST(FileName AS VARCHAR(MAX)) AS FileName,
		CAST(ReferringCareUnit AS VARCHAR(MAX)) AS ReferringCareUnit,
		CAST(ReferringClinic AS VARCHAR(MAX)) AS ReferringClinic,
		CAST(ReferringHospital AS VARCHAR(MAX)) AS ReferringHospital,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TransactionID AS VARCHAR(MAX)) AS TransactionID 
	FROM Intelligence.viewreader.vGVR_OutpatientDiagnoses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    