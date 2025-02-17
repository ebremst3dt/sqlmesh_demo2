
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'MOTFRA_GILTIG_FOM': 'datetime',
 'MOTFRA_GILTIG_TOM': 'datetime',
 'MOTFRA_ID': 'varchar(4)',
 'MOTFRA_ID_TEXT': 'varchar(35)',
 'MOTFRA_PASSIV': 'bit',
 'MOTFRA_TEXT': 'varchar(30)',
 'MOTP_GILTIG_FOM': 'datetime',
 'MOTP_GILTIG_TOM': 'datetime',
 'MOTP_ID': 'varchar(4)',
 'MOTP_ID_TEXT': 'varchar(35)',
 'MOTP_PASSIV': 'bit',
 'MOTP_TEXT': 'varchar(30)'},
    kind=ModelKindName.FULL,
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """
	SELECT top 1000
 		CONVERT(varchar(max), MOTFRA_GILTIG_FOM, 126) AS motfra_giltig_fom,
		CONVERT(varchar(max), MOTFRA_GILTIG_TOM, 126) AS motfra_giltig_tom,
		CAST(MOTFRA_ID AS VARCHAR(MAX)) AS motfra_id,
		CAST(MOTFRA_ID_TEXT AS VARCHAR(MAX)) AS motfra_id_text,
		CAST(MOTFRA_PASSIV AS VARCHAR(MAX)) AS motfra_passiv,
		CAST(MOTFRA_TEXT AS VARCHAR(MAX)) AS motfra_text,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS motp_giltig_fom,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS motp_giltig_tom,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS motp_id_text,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS motp_passiv,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS motp_text 
	FROM utdata.utdata295.EK_DIM_OBJ_MOTP_DIR_28
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
