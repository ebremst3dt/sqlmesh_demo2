
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'MPAYID_GILTIG_FOM': 'varchar(max)', 'MPAYID_GILTIG_TOM': 'varchar(max)', 'MPAYID_ID': 'varchar(max)', 'MPAYID_ID_TEXT': 'varchar(max)', 'MPAYID_PASSIV': 'varchar(max)', 'MPAYID_TEXT': 'varchar(max)'},
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
		CONVERT(varchar(max), MPAYID_GILTIG_FOM, 126) AS mpayid_giltig_fom,
		CONVERT(varchar(max), MPAYID_GILTIG_TOM, 126) AS mpayid_giltig_tom,
		CAST(MPAYID_ID AS VARCHAR(MAX)) AS mpayid_id,
		CAST(MPAYID_ID_TEXT AS VARCHAR(MAX)) AS mpayid_id_text,
		CAST(MPAYID_PASSIV AS VARCHAR(MAX)) AS mpayid_passiv,
		CAST(MPAYID_TEXT AS VARCHAR(MAX)) AS mpayid_text 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_MPAYID ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    