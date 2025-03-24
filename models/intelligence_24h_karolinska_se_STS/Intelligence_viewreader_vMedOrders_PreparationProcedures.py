
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Iordningställanden av läkemedel. Läkemedel som tas enligt behandlingsschema eller vid behov sparas ej.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BatchNumber': 'varchar(max)', 'Comment': 'varchar(max)', 'DocumentID': 'varchar(max)', 'OrderCreatedAtCareUnitID': 'varchar(max)', 'OrderDoseText': 'varchar(max)', 'OrderDoseTextSolution': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparedDatetime': 'varchar(max)', 'PrescriptionDate': 'varchar(max)', 'PrescriptionTime': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'Substitute': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)', 'TreatmentReason': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'TimestampSaved': "{'title_ui': 'Registrerad', 'description': 'Tidpunkt då denna iordningställning sparades'}", 'SavedByUserID': "{'title_ui': None, 'description': 'Användaren som sparat data'}", 'SavedAtCareUnitID': "{'title_ui': None, 'description': 'Där data är sparat'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar till iordningsställandet'}", 'TreatmentReason': "{'title_ui': 'Behandlingsorsak', 'description': 'Varför preparatet iordningställdes'}", 'PreparedDatetime': "{'title_ui': 'Iordningställd/Iordn.datum, Iordn.tid', 'description': 'Händelsetid för iordningsställande, dvs då iordningställande uppges ha skett'}", 'PrescriptionDate': "{'title_ui': None, 'description': 'Det datum som iordningställningen avser. Används för att koppla ihop med eventuell infusion.'}", 'PrescriptionTime': "{'title_ui': None, 'description': 'Det klockslag som iordningställningen avser. Används för att koppla ihop med eventuell infusion.'}", 'OrderCreatedAtCareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet där dokumentet är skapat. Den vårdenhet som behörighet utgår från.'}", 'OrderDoseText': "{'title_ui': 'Ord dos', 'description': 'Ordinerad dos/mängd med enhet för samtliga preparat i ordinationen.'}", 'OrderDoseTextSolution': "{'title_ui': 'Ord dos', 'description': 'Ordinerad dos av bruksfärdig lösning.'}", 'BatchNumber': "{'title_ui': 'Batchnummer', 'description': 'Batchnummer på iordningställt preparat'}", 'Substitute': "{'title_ui': 'Utbytt till preparat', 'description': 'Iordningsställt preparat om annat än ordinerat'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([BatchNumber] AS VARCHAR(MAX)) AS [BatchNumber],
		CAST([Comment] AS VARCHAR(MAX)) AS [Comment],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([OrderCreatedAtCareUnitID] AS VARCHAR(MAX)) AS [OrderCreatedAtCareUnitID],
		CAST([OrderDoseText] AS VARCHAR(MAX)) AS [OrderDoseText],
		CAST([OrderDoseTextSolution] AS VARCHAR(MAX)) AS [OrderDoseTextSolution],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [PreparedDatetime], 126) AS [PreparedDatetime],
		CONVERT(varchar(max), [PrescriptionDate], 126) AS [PrescriptionDate],
		CONVERT(varchar(max), [PrescriptionTime], 126) AS [PrescriptionTime],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SavedAtCareUnitID] AS VARCHAR(MAX)) AS [SavedAtCareUnitID],
		CAST([SavedByUserID] AS VARCHAR(MAX)) AS [SavedByUserID],
		CAST([Substitute] AS VARCHAR(MAX)) AS [Substitute],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CONVERT(varchar(max), [TimestampSaved], 126) AS [TimestampSaved],
		CAST([TreatmentReason] AS VARCHAR(MAX)) AS [TreatmentReason] 
	FROM Intelligence.viewreader.vMedOrders_PreparationProcedures) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    