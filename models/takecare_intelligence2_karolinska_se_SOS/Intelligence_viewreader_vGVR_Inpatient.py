
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""GVR-transaktioner för slutenvården. Om de sex fälten (GVRs interna nyckelfält) PatientID, EventDate, EventTime, Hospital, Clinic och CareUnit har samma värde för flera slutenvårdstransaktioner så hör transaktionerna ihop.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'CareUnit': 'varchar(max)', 'Clinic': 'varchar(max)', 'EconomicalKombika': 'varchar(max)', 'EventDate': 'varchar(max)', 'EventDatetime': 'varchar(max)', 'EventTime': 'varchar(max)', 'FileName': 'varchar(max)', 'Hospital': 'varchar(max)', 'IsCancellation': 'varchar(max)', 'IsCorrection': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'PatientID': "{'title_ui': None, 'description': 'Patientens Person-/reservnummer'}", 'EventDate': "{'title_ui': None, 'description': 'Vårdhändelsedatum. Datum för inskrivning.'}", 'EventTime': "{'title_ui': None, 'description': 'Vårdhändelsetid. Tid för inskrivning.'}", 'EventDatetime': "{'title_ui': None, 'description': 'EventDate och EventTime är hopslagna till detta fält för att kunna matchas mot PAS'}", 'Hospital': "{'title_ui': None, 'description': 'Vårdande inrättning. Ekonomisk enhet. Första delen av kombika.'}", 'Clinic': "{'title_ui': None, 'description': 'Vårdande klinik. Ekonomisk enhet. Andra delen av kombika.'}", 'CareUnit': "{'title_ui': None, 'description': 'Vårdande avdelning. Ekonomisk enhet. Tredje delen av kombika.'}", 'EconomicalKombika': "{'title_ui': None, 'description': 'Kolumnerna Hospital, Clinic och CareUnit sammansatta till ett kombikafält för att kunna matchas mot PAS.'}", 'IsCancellation': "{'title_ui': None, 'description': 'Denna transaktion tar bort en inskrivning. Denna kolumn är skapad internt, utgående från transaktionstyps-id i kolumnen GVR.InternalGVRServiceTypeID.'}", 'IsCorrection': "{'title_ui': None, 'description': 'Denna transaktion korrigerar en inskrivning. Allt data sänds på nytt. Denna kolumn är skapad internt, utgående från transaktionstyps-id i kolumnen GVR.InternalGVRServiceTypeID.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(CareUnit AS VARCHAR(MAX)) AS CareUnit,
		CAST(Clinic AS VARCHAR(MAX)) AS Clinic,
		CAST(EconomicalKombika AS VARCHAR(MAX)) AS EconomicalKombika,
		CONVERT(varchar(max), EventDate, 126) AS EventDate,
		CONVERT(varchar(max), EventDatetime, 126) AS EventDatetime,
		CONVERT(varchar(max), EventTime, 126) AS EventTime,
		CAST(FileName AS VARCHAR(MAX)) AS FileName,
		CAST(Hospital AS VARCHAR(MAX)) AS Hospital,
		CAST(IsCancellation AS VARCHAR(MAX)) AS IsCancellation,
		CAST(IsCorrection AS VARCHAR(MAX)) AS IsCorrection,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TransactionID AS VARCHAR(MAX)) AS TransactionID 
	FROM Intelligence.viewreader.vGVR_Inpatient) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    