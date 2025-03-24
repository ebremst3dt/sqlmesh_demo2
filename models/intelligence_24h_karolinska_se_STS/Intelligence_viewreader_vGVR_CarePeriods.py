
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="GVR-transaktioner som hör ihop med en vårdperiod. Om de två fälten (GVRs interna nyckelfält) PatientID och CarePeriodCode har samma värde för flera Vårdperiodtransaktioner så hör transaktionerna ihop.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'CarePeriodCode': 'varchar(max)', 'EconomicalKombika': 'varchar(max)', 'EventDatetime': 'varchar(max)', 'FileName': 'varchar(max)', 'IsCancellation': 'varchar(max)', 'IsCorrection': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'PatientID': "{'title_ui': None, 'description': 'Patientens Person-/reservnummer'}", 'CarePeriodCode': "{'title_ui': None, 'description': 'Vårdperiod-id. Kombika plus ett internt löpnummer som består av godtyckligt ledigt datum'}", 'EventDatetime': "{'title_ui': None, 'description': 'Händelsedatum. Datatypen är dock datetime för att möjliggöra koppling till PAS. Datumet tolkas som startdatum för start- och borttagningstransaktioner för vårdperiod, men som slutdatum för avslut av vårdperiod'}", 'EconomicalKombika': "{'title_ui': None, 'description': 'Ekonomisk kombika extraherad ur kolumnen CarePeriodCodes för att kunna matchas mot PAS.'}", 'IsCancellation': "{'title_ui': None, 'description': 'Denna transaktion tar bort en vårdperiod. Denna kolumn är skapad internt, utgående från transaktionstyps-id i kolumnen GVR.InternalGVRServiceTypeID.'}", 'IsCorrection': "{'title_ui': None, 'description': 'Denna transaktion korrigerar en Vårdperiod. Allt data sänds på nytt. Denna kolumn är skapad internt, utgående från transaktionstyps-id i kolumnen GVR.InternalGVRServiceTypeID.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST([CarePeriodCode] AS VARCHAR(MAX)) AS [CarePeriodCode],
		CAST([EconomicalKombika] AS VARCHAR(MAX)) AS [EconomicalKombika],
		CONVERT(varchar(max), [EventDatetime], 126) AS [EventDatetime],
		CAST([FileName] AS VARCHAR(MAX)) AS [FileName],
		CAST([IsCancellation] AS VARCHAR(MAX)) AS [IsCancellation],
		CAST([IsCorrection] AS VARCHAR(MAX)) AS [IsCorrection],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([TransactionID] AS VARCHAR(MAX)) AS [TransactionID] 
	FROM Intelligence.viewreader.vGVR_CarePeriods) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    