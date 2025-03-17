
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DIVMP_GILTIG_FOM': 'varchar(max)', 'DIVMP_GILTIG_TOM': 'varchar(max)', 'DIVMP_ID': 'varchar(max)', 'DIVMP_ID_TEXT': 'varchar(max)', 'DIVMP_PASSIV': 'varchar(max)', 'DIVMP_TEXT': 'varchar(max)', 'EKOD_GILTIG_FOM': 'varchar(max)', 'EKOD_GILTIG_TOM': 'varchar(max)', 'EKOD_ID': 'varchar(max)', 'EKOD_ID_TEXT': 'varchar(max)', 'EKOD_PASSIV': 'varchar(max)', 'EKOD_TEXT': 'varchar(max)', 'IMOTPB_GILTIG_FOM': 'varchar(max)', 'IMOTPB_GILTIG_TOM': 'varchar(max)', 'IMOTPB_ID': 'varchar(max)', 'IMOTPB_ID_TEXT': 'varchar(max)', 'IMOTPB_PASSIV': 'varchar(max)', 'IMOTPB_TEXT': 'varchar(max)', 'KMOTP_GILTIG_FOM': 'varchar(max)', 'KMOTP_GILTIG_TOM': 'varchar(max)', 'KMOTP_ID': 'varchar(max)', 'KMOTP_ID_TEXT': 'varchar(max)', 'KMOTP_PASSIV': 'varchar(max)', 'KMOTP_TEXT': 'varchar(max)', 'MOTP_GILTIG_FOM': 'varchar(max)', 'MOTP_GILTIG_TOM': 'varchar(max)', 'MOTP_ID': 'varchar(max)', 'MOTP_ID_TEXT': 'varchar(max)', 'MOTP_PASSIV': 'varchar(max)', 'MOTP_TEXT': 'varchar(max)'},
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
		'step_rd_sll_se_steudp_udp_600' as _source,
		CONVERT(varchar(max), DIVMP_GILTIG_FOM, 126) AS divmp_giltig_fom,
		CONVERT(varchar(max), DIVMP_GILTIG_TOM, 126) AS divmp_giltig_tom,
		CAST(DIVMP_ID AS VARCHAR(MAX)) AS divmp_id,
		CAST(DIVMP_ID_TEXT AS VARCHAR(MAX)) AS divmp_id_text,
		CAST(DIVMP_PASSIV AS VARCHAR(MAX)) AS divmp_passiv,
		CAST(DIVMP_TEXT AS VARCHAR(MAX)) AS divmp_text,
		CONVERT(varchar(max), EKOD_GILTIG_FOM, 126) AS ekod_giltig_fom,
		CONVERT(varchar(max), EKOD_GILTIG_TOM, 126) AS ekod_giltig_tom,
		CAST(EKOD_ID AS VARCHAR(MAX)) AS ekod_id,
		CAST(EKOD_ID_TEXT AS VARCHAR(MAX)) AS ekod_id_text,
		CAST(EKOD_PASSIV AS VARCHAR(MAX)) AS ekod_passiv,
		CAST(EKOD_TEXT AS VARCHAR(MAX)) AS ekod_text,
		CONVERT(varchar(max), IMOTPB_GILTIG_FOM, 126) AS imotpb_giltig_fom,
		CONVERT(varchar(max), IMOTPB_GILTIG_TOM, 126) AS imotpb_giltig_tom,
		CAST(IMOTPB_ID AS VARCHAR(MAX)) AS imotpb_id,
		CAST(IMOTPB_ID_TEXT AS VARCHAR(MAX)) AS imotpb_id_text,
		CAST(IMOTPB_PASSIV AS VARCHAR(MAX)) AS imotpb_passiv,
		CAST(IMOTPB_TEXT AS VARCHAR(MAX)) AS imotpb_text,
		CONVERT(varchar(max), KMOTP_GILTIG_FOM, 126) AS kmotp_giltig_fom,
		CONVERT(varchar(max), KMOTP_GILTIG_TOM, 126) AS kmotp_giltig_tom,
		CAST(KMOTP_ID AS VARCHAR(MAX)) AS kmotp_id,
		CAST(KMOTP_ID_TEXT AS VARCHAR(MAX)) AS kmotp_id_text,
		CAST(KMOTP_PASSIV AS VARCHAR(MAX)) AS kmotp_passiv,
		CAST(KMOTP_TEXT AS VARCHAR(MAX)) AS kmotp_text,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS motp_giltig_fom,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS motp_giltig_tom,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS motp_id_text,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS motp_passiv,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS motp_text 
	FROM steudp.udp_600.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    