
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MOTFRA_GILTIG_FOM': 'varchar(max)', 'MOTFRA_GILTIG_TOM': 'varchar(max)', 'MOTFRA_ID': 'varchar(max)', 'MOTFRA_ID_TEXT': 'varchar(max)', 'MOTFRA_PASSIV': 'varchar(max)', 'MOTFRA_TEXT': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), MOTFRA_GILTIG_FOM, 126) AS MOTFRA_GILTIG_FOM,
		CONVERT(varchar(max), MOTFRA_GILTIG_TOM, 126) AS MOTFRA_GILTIG_TOM,
		CAST(MOTFRA_ID AS VARCHAR(MAX)) AS MOTFRA_ID,
		CAST(MOTFRA_ID_TEXT AS VARCHAR(MAX)) AS MOTFRA_ID_TEXT,
		CAST(MOTFRA_PASSIV AS VARCHAR(MAX)) AS MOTFRA_PASSIV,
		CAST(MOTFRA_TEXT AS VARCHAR(MAX)) AS MOTFRA_TEXT,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS MOTP_GILTIG_FOM,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS MOTP_GILTIG_TOM,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT 
	FROM utdata.utdata287.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    