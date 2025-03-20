
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""GVR utskrivningstransaktioner och korrigeringar av utskrivning, för slutenvården.""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'DeceasedTime': 'varchar(max)', 'DischargeCode': 'varchar(max)', 'DischargeDate': 'varchar(max)', 'DischargeFormCode': 'varchar(max)', 'DischargingCareUnit': 'varchar(max)', 'DischargingClinic': 'varchar(max)', 'DischargingHospital': 'varchar(max)', 'FileName': 'varchar(max)', 'PaymentDueDate': 'varchar(max)', 'ReferredToClinic': 'varchar(max)', 'ReferredToHospital': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'TransactionID': 'varchar(max)', 'TreatmentCompletedDate': 'varchar(max)'},
    column_descriptions={'FileName': "{'title_ui': None, 'description': 'Namnet på den GVR-loggfil (komponentfil) varifrån datat hämtats'}", 'TransactionID': "{'title_ui': None, 'description': 'Internt id som identifierar transaktionen i filen'}", 'ReferredToHospital': "{'title_ui': None, 'description': 'Remitterad till inrättning. Första delen av kombika.'}", 'ReferredToClinic': "{'title_ui': None, 'description': 'Remitterad till klinik. Andra delen av kombika.'}", 'DischargeDate': "{'title_ui': None, 'description': 'Utskrivningsdatum'}", 'DischargingCareUnit': "{'title_ui': None, 'description': 'Utskrivande avdelning. Ekonomisk enhet. Tredje delen av kombika.'}", 'DischargingHospital': "{'title_ui': None, 'description': 'Utskrivande inrättning. Ekonomisk enhet. Första delen av kombika.'}", 'DischargingClinic': "{'title_ui': None, 'description': 'Utskrivande klinik. Ekonomisk enhet. Andra delen av kombika.'}", 'DischargeCode': "{'title_ui': None, 'description': 'Utskrivningskod'}", 'DischargeFormCode': "{'title_ui': None, 'description': 'Utskrivningsformskod'}", 'DeceasedTime': "{'title_ui': None, 'description': 'Dödstidpunkt, klockslag'}", 'TreatmentCompletedDate': "{'title_ui': None, 'description': 'Färdigbehandlad datum'}", 'PaymentDueDate': "{'title_ui': None, 'description': 'Betalningsansvarig datum'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CONVERT(varchar(max), DeceasedTime, 126) AS DeceasedTime,
		CAST(DischargeCode AS VARCHAR(MAX)) AS DischargeCode,
		CONVERT(varchar(max), DischargeDate, 126) AS DischargeDate,
		CAST(DischargeFormCode AS VARCHAR(MAX)) AS DischargeFormCode,
		CAST(DischargingCareUnit AS VARCHAR(MAX)) AS DischargingCareUnit,
		CAST(DischargingClinic AS VARCHAR(MAX)) AS DischargingClinic,
		CAST(DischargingHospital AS VARCHAR(MAX)) AS DischargingHospital,
		CAST(FileName AS VARCHAR(MAX)) AS FileName,
		CONVERT(varchar(max), PaymentDueDate, 126) AS PaymentDueDate,
		CAST(ReferredToClinic AS VARCHAR(MAX)) AS ReferredToClinic,
		CAST(ReferredToHospital AS VARCHAR(MAX)) AS ReferredToHospital,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(TransactionID AS VARCHAR(MAX)) AS TransactionID,
		CONVERT(varchar(max), TreatmentCompletedDate, 126) AS TreatmentCompletedDate 
	FROM Intelligence.viewreader.vGVR_InpatientDischarges) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    