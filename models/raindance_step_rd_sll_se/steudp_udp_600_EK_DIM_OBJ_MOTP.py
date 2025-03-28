
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
		CONVERT(varchar(max), DIVMP_GILTIG_FOM, 126) AS DIVMP_GILTIG_FOM,
		CONVERT(varchar(max), DIVMP_GILTIG_TOM, 126) AS DIVMP_GILTIG_TOM,
		CAST(DIVMP_ID AS VARCHAR(MAX)) AS DIVMP_ID,
		CAST(DIVMP_ID_TEXT AS VARCHAR(MAX)) AS DIVMP_ID_TEXT,
		CAST(DIVMP_PASSIV AS VARCHAR(MAX)) AS DIVMP_PASSIV,
		CAST(DIVMP_TEXT AS VARCHAR(MAX)) AS DIVMP_TEXT,
		CONVERT(varchar(max), EKOD_GILTIG_FOM, 126) AS EKOD_GILTIG_FOM,
		CONVERT(varchar(max), EKOD_GILTIG_TOM, 126) AS EKOD_GILTIG_TOM,
		CAST(EKOD_ID AS VARCHAR(MAX)) AS EKOD_ID,
		CAST(EKOD_ID_TEXT AS VARCHAR(MAX)) AS EKOD_ID_TEXT,
		CAST(EKOD_PASSIV AS VARCHAR(MAX)) AS EKOD_PASSIV,
		CAST(EKOD_TEXT AS VARCHAR(MAX)) AS EKOD_TEXT,
		CONVERT(varchar(max), IMOTPB_GILTIG_FOM, 126) AS IMOTPB_GILTIG_FOM,
		CONVERT(varchar(max), IMOTPB_GILTIG_TOM, 126) AS IMOTPB_GILTIG_TOM,
		CAST(IMOTPB_ID AS VARCHAR(MAX)) AS IMOTPB_ID,
		CAST(IMOTPB_ID_TEXT AS VARCHAR(MAX)) AS IMOTPB_ID_TEXT,
		CAST(IMOTPB_PASSIV AS VARCHAR(MAX)) AS IMOTPB_PASSIV,
		CAST(IMOTPB_TEXT AS VARCHAR(MAX)) AS IMOTPB_TEXT,
		CONVERT(varchar(max), KMOTP_GILTIG_FOM, 126) AS KMOTP_GILTIG_FOM,
		CONVERT(varchar(max), KMOTP_GILTIG_TOM, 126) AS KMOTP_GILTIG_TOM,
		CAST(KMOTP_ID AS VARCHAR(MAX)) AS KMOTP_ID,
		CAST(KMOTP_ID_TEXT AS VARCHAR(MAX)) AS KMOTP_ID_TEXT,
		CAST(KMOTP_PASSIV AS VARCHAR(MAX)) AS KMOTP_PASSIV,
		CAST(KMOTP_TEXT AS VARCHAR(MAX)) AS KMOTP_TEXT,
		CONVERT(varchar(max), MOTP_GILTIG_FOM, 126) AS MOTP_GILTIG_FOM,
		CONVERT(varchar(max), MOTP_GILTIG_TOM, 126) AS MOTP_GILTIG_TOM,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
		CAST(MOTP_ID_TEXT AS VARCHAR(MAX)) AS MOTP_ID_TEXT,
		CAST(MOTP_PASSIV AS VARCHAR(MAX)) AS MOTP_PASSIV,
		CAST(MOTP_TEXT AS VARCHAR(MAX)) AS MOTP_TEXT 
	FROM steudp.udp_600.EK_DIM_OBJ_MOTP ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    