
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="""""",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AnswerCode': 'varchar(max)', 'AnswerInstanceCode': 'varchar(max)', 'DocumentID': 'varchar(max)', 'PartialAnswerCode': 'varchar(max)', 'PartialAnswerValue': 'varchar(max)', 'PatientID': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={},
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
		CAST(AnswerCode AS VARCHAR(MAX)) AS AnswerCode,
		CAST(AnswerInstanceCode AS VARCHAR(MAX)) AS AnswerInstanceCode,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(PartialAnswerCode AS VARCHAR(MAX)) AS PartialAnswerCode,
		CAST(PartialAnswerValue AS VARCHAR(MAX)) AS PartialAnswerValue,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vWebcertCertificates_Answers) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    