
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BOANTP_GILTIG_FOM': 'varchar(max)', 'BOANTP_GILTIG_TOM': 'varchar(max)', 'BOANTP_ID': 'varchar(max)', 'BOANTP_ID_TEXT': 'varchar(max)', 'BOANTP_PASSIV': 'varchar(max)', 'BOANTP_TEXT': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.FULL
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
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata287' as _source,
		CONVERT(varchar(max), BOANTP_GILTIG_FOM, 126) AS boantp_giltig_fom,
		CONVERT(varchar(max), BOANTP_GILTIG_TOM, 126) AS boantp_giltig_tom,
		CAST(BOANTP_ID AS VARCHAR(MAX)) AS boantp_id,
		CAST(BOANTP_ID_TEXT AS VARCHAR(MAX)) AS boantp_id_text,
		CAST(BOANTP_PASSIV AS VARCHAR(MAX)) AS boantp_passiv,
		CAST(BOANTP_TEXT AS VARCHAR(MAX)) AS boantp_text 
	FROM utdata.utdata287.EK_DIM_OBJ_BOANTP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    