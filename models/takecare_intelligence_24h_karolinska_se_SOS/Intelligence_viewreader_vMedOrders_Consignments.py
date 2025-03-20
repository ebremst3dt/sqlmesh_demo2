
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Överlämning av läkemedel till patient eller en tredje person. Läkemedel som tas enligt behandlingsschema eller vid behov sparas ej.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Comment': 'varchar(max)', 'ConsignedDatetime': 'varchar(max)', 'ConsignmentDate': 'varchar(max)', 'ConsignmentRecipientName': 'varchar(max)', 'ConsignmentRecipientTypeID': 'varchar(max)', 'ConsignmentTime': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Dose': 'varchar(max)', 'OrderCreatedAtCareUnitID': 'varchar(max)', 'OrderDoseText': 'varchar(max)', 'OrderDoseTextSolution': 'varchar(max)', 'PatientID': 'varchar(max)', 'PreparationNo': 'varchar(max)', 'Row': 'varchar(max)', 'SavedAtCareUnitID': 'varchar(max)', 'SavedByUserID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TimestampSaved': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'TimestampSaved': "{'title_ui': 'Registrerad', 'description': 'Tidpunkt då denna överlämning sparades'}", 'SavedByUserID': "{'title_ui': 'Överl av/Registrerad av', 'description': 'Senast ändrad av'}", 'SavedAtCareUnitID': "{'title_ui': 'Vårdenhet', 'description': 'Där data är sparat.'}", 'ConsignedDatetime': "{'title_ui': 'Överlämnad/Överl.datum, Överl.tid', 'description': 'Händelsetid för överlämningen, dvs då överlämningen uppges ha skett'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Kommentar till överlämningen'}", 'ConsignmentDate': "{'title_ui': 'Ord.datum', 'description': 'Det datum som överlämningen avser. Används för att koppla ihop med eventuell infusion.'}", 'ConsignmentTime': "{'title_ui': 'Ord.tid', 'description': 'Det klockslag som överlämningen avser. Används för att koppla ihop med eventuell infusion.'}", 'OrderCreatedAtCareUnitID': "{'title_ui': None, 'description': 'Den vårdenhet där ordinationen är skapad. Vårdenheten som behörighet utgår från.'}", 'OrderDoseText': "{'title_ui': 'Ord dos', 'description': 'Ordinerad dos/mängd med enhet för samtliga preparat i ordinationen.'}", 'OrderDoseTextSolution': "{'title_ui': 'Ordinerad dos/Ord.dos', 'description': 'Ordinerad dos av bruksfärdig lösning.'}", 'ConsignmentRecipientTypeID': "{'title_ui': 'Överlämnad till, Patienten, Annan', 'description': {'break': [None, None]}}", 'ConsignmentRecipientName': '{\'title_ui\': \'Annan\', \'description\': \'Namnet på den som mottagit dosen, sparas endast om "Annan" får dosen\'}', 'PreparationNo': "{'title_ui': None, 'description': 'ID för preparatet i listan (löpnummer)'}", 'Dose': "{'title_ui': 'Överl dos', 'description': 'Överlämnad dos'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CONVERT(varchar(max), ConsignedDatetime, 126) AS ConsignedDatetime,
		CONVERT(varchar(max), ConsignmentDate, 126) AS ConsignmentDate,
		CAST(ConsignmentRecipientName AS VARCHAR(MAX)) AS ConsignmentRecipientName,
		CAST(ConsignmentRecipientTypeID AS VARCHAR(MAX)) AS ConsignmentRecipientTypeID,
		CONVERT(varchar(max), ConsignmentTime, 126) AS ConsignmentTime,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(Dose AS VARCHAR(MAX)) AS Dose,
		CAST(OrderCreatedAtCareUnitID AS VARCHAR(MAX)) AS OrderCreatedAtCareUnitID,
		CAST(OrderDoseText AS VARCHAR(MAX)) AS OrderDoseText,
		CAST(OrderDoseTextSolution AS VARCHAR(MAX)) AS OrderDoseTextSolution,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(PreparationNo AS VARCHAR(MAX)) AS PreparationNo,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SavedAtCareUnitID AS VARCHAR(MAX)) AS SavedAtCareUnitID,
		CAST(SavedByUserID AS VARCHAR(MAX)) AS SavedByUserID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), TimestampSaved, 126) AS TimestampSaved 
	FROM Intelligence.viewreader.vMedOrders_Consignments) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    