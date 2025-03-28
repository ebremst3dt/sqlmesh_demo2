
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSELI_GILTIG_FOM': 'varchar(max)', 'ANSELI_GILTIG_TOM': 'varchar(max)', 'ANSELI_ID': 'varchar(max)', 'ANSELI_ID_TEXT': 'varchar(max)', 'ANSELI_PASSIV': 'varchar(max)', 'ANSELI_TEXT': 'varchar(max)', 'ELSJKH_GILTIG_FOM': 'varchar(max)', 'ELSJKH_GILTIG_TOM': 'varchar(max)', 'ELSJKH_ID': 'varchar(max)', 'ELSJKH_ID_TEXT': 'varchar(max)', 'ELSJKH_PASSIV': 'varchar(max)', 'ELSJKH_TEXT': 'varchar(max)', 'FRAGRP_GILTIG_FOM': 'varchar(max)', 'FRAGRP_GILTIG_TOM': 'varchar(max)', 'FRAGRP_ID': 'varchar(max)', 'FRAGRP_ID_TEXT': 'varchar(max)', 'FRAGRP_PASSIV': 'varchar(max)', 'FRAGRP_TEXT': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'IK_GILTIG_FOM': 'varchar(max)', 'IK_GILTIG_TOM': 'varchar(max)', 'IK_ID': 'varchar(max)', 'IK_ID_TEXT': 'varchar(max)', 'IK_PASSIV': 'varchar(max)', 'IK_TEXT': 'varchar(max)', 'KGRUPP_GILTIG_FOM': 'varchar(max)', 'KGRUPP_GILTIG_TOM': 'varchar(max)', 'KGRUPP_ID': 'varchar(max)', 'KGRUPP_ID_TEXT': 'varchar(max)', 'KGRUPP_PASSIV': 'varchar(max)', 'KGRUPP_TEXT': 'varchar(max)', 'KKLASS_GILTIG_FOM': 'varchar(max)', 'KKLASS_GILTIG_TOM': 'varchar(max)', 'KKLASS_ID': 'varchar(max)', 'KKLASS_ID_TEXT': 'varchar(max)', 'KKLASS_PASSIV': 'varchar(max)', 'KKLASS_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'RAD_GILTIG_FOM': 'varchar(max)', 'RAD_GILTIG_TOM': 'varchar(max)', 'RAD_ID': 'varchar(max)', 'RAD_ID_TEXT': 'varchar(max)', 'RAD_PASSIV': 'varchar(max)', 'RAD_TEXT': 'varchar(max)', 'RR_STS_GILTIG_FOM': 'varchar(max)', 'RR_STS_GILTIG_TOM': 'varchar(max)', 'RR_STS_ID': 'varchar(max)', 'RR_STS_ID_TEXT': 'varchar(max)', 'RR_STS_PASSIV': 'varchar(max)', 'RR_STS_TEXT': 'varchar(max)', 'ÅRL_GILTIG_FOM': 'varchar(max)', 'ÅRL_GILTIG_TOM': 'varchar(max)', 'ÅRL_ID': 'varchar(max)', 'ÅRL_ID_TEXT': 'varchar(max)', 'ÅRL_PASSIV': 'varchar(max)', 'ÅRL_TEXT': 'varchar(max)'},
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
		'stsp_rd_sll_se_stsudp_udp_858' as _source,
		CONVERT(varchar(max), ANSELI_GILTIG_FOM, 126) AS ANSELI_GILTIG_FOM,
		CONVERT(varchar(max), ANSELI_GILTIG_TOM, 126) AS ANSELI_GILTIG_TOM,
		CAST(ANSELI_ID AS VARCHAR(MAX)) AS ANSELI_ID,
		CAST(ANSELI_ID_TEXT AS VARCHAR(MAX)) AS ANSELI_ID_TEXT,
		CAST(ANSELI_PASSIV AS VARCHAR(MAX)) AS ANSELI_PASSIV,
		CAST(ANSELI_TEXT AS VARCHAR(MAX)) AS ANSELI_TEXT,
		CONVERT(varchar(max), ELSJKH_GILTIG_FOM, 126) AS ELSJKH_GILTIG_FOM,
		CONVERT(varchar(max), ELSJKH_GILTIG_TOM, 126) AS ELSJKH_GILTIG_TOM,
		CAST(ELSJKH_ID AS VARCHAR(MAX)) AS ELSJKH_ID,
		CAST(ELSJKH_ID_TEXT AS VARCHAR(MAX)) AS ELSJKH_ID_TEXT,
		CAST(ELSJKH_PASSIV AS VARCHAR(MAX)) AS ELSJKH_PASSIV,
		CAST(ELSJKH_TEXT AS VARCHAR(MAX)) AS ELSJKH_TEXT,
		CONVERT(varchar(max), FRAGRP_GILTIG_FOM, 126) AS FRAGRP_GILTIG_FOM,
		CONVERT(varchar(max), FRAGRP_GILTIG_TOM, 126) AS FRAGRP_GILTIG_TOM,
		CAST(FRAGRP_ID AS VARCHAR(MAX)) AS FRAGRP_ID,
		CAST(FRAGRP_ID_TEXT AS VARCHAR(MAX)) AS FRAGRP_ID_TEXT,
		CAST(FRAGRP_PASSIV AS VARCHAR(MAX)) AS FRAGRP_PASSIV,
		CAST(FRAGRP_TEXT AS VARCHAR(MAX)) AS FRAGRP_TEXT,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS FRANGO_GILTIG_FOM,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS FRANGO_GILTIG_TOM,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS FRANGO_ID,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS FRANGO_ID_TEXT,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS FRANGO_PASSIV,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS FRANGO_TEXT,
		CONVERT(varchar(max), IK_GILTIG_FOM, 126) AS IK_GILTIG_FOM,
		CONVERT(varchar(max), IK_GILTIG_TOM, 126) AS IK_GILTIG_TOM,
		CAST(IK_ID AS VARCHAR(MAX)) AS IK_ID,
		CAST(IK_ID_TEXT AS VARCHAR(MAX)) AS IK_ID_TEXT,
		CAST(IK_PASSIV AS VARCHAR(MAX)) AS IK_PASSIV,
		CAST(IK_TEXT AS VARCHAR(MAX)) AS IK_TEXT,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS KGRUPP_GILTIG_FOM,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS KGRUPP_GILTIG_TOM,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS KGRUPP_ID,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS KGRUPP_ID_TEXT,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS KGRUPP_PASSIV,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS KGRUPP_TEXT,
		CONVERT(varchar(max), KKLASS_GILTIG_FOM, 126) AS KKLASS_GILTIG_FOM,
		CONVERT(varchar(max), KKLASS_GILTIG_TOM, 126) AS KKLASS_GILTIG_TOM,
		CAST(KKLASS_ID AS VARCHAR(MAX)) AS KKLASS_ID,
		CAST(KKLASS_ID_TEXT AS VARCHAR(MAX)) AS KKLASS_ID_TEXT,
		CAST(KKLASS_PASSIV AS VARCHAR(MAX)) AS KKLASS_PASSIV,
		CAST(KKLASS_TEXT AS VARCHAR(MAX)) AS KKLASS_TEXT,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS KONTO_GILTIG_FOM,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS KONTO_GILTIG_TOM,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS KONTO_ID_TEXT,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS KONTO_PASSIV,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS KONTO_TEXT,
		CONVERT(varchar(max), RAD_GILTIG_FOM, 126) AS RAD_GILTIG_FOM,
		CONVERT(varchar(max), RAD_GILTIG_TOM, 126) AS RAD_GILTIG_TOM,
		CAST(RAD_ID AS VARCHAR(MAX)) AS RAD_ID,
		CAST(RAD_ID_TEXT AS VARCHAR(MAX)) AS RAD_ID_TEXT,
		CAST(RAD_PASSIV AS VARCHAR(MAX)) AS RAD_PASSIV,
		CAST(RAD_TEXT AS VARCHAR(MAX)) AS RAD_TEXT,
		CONVERT(varchar(max), RR_STS_GILTIG_FOM, 126) AS RR_STS_GILTIG_FOM,
		CONVERT(varchar(max), RR_STS_GILTIG_TOM, 126) AS RR_STS_GILTIG_TOM,
		CAST(RR_STS_ID AS VARCHAR(MAX)) AS RR_STS_ID,
		CAST(RR_STS_ID_TEXT AS VARCHAR(MAX)) AS RR_STS_ID_TEXT,
		CAST(RR_STS_PASSIV AS VARCHAR(MAX)) AS RR_STS_PASSIV,
		CAST(RR_STS_TEXT AS VARCHAR(MAX)) AS RR_STS_TEXT,
		CONVERT(varchar(max), ÅRL_GILTIG_FOM, 126) AS ÅRL_GILTIG_FOM,
		CONVERT(varchar(max), ÅRL_GILTIG_TOM, 126) AS ÅRL_GILTIG_TOM,
		CAST(ÅRL_ID AS VARCHAR(MAX)) AS ÅRL_ID,
		CAST(ÅRL_ID_TEXT AS VARCHAR(MAX)) AS ÅRL_ID_TEXT,
		CAST(ÅRL_PASSIV AS VARCHAR(MAX)) AS ÅRL_PASSIV,
		CAST(ÅRL_TEXT AS VARCHAR(MAX)) AS ÅRL_TEXT 
	FROM stsudp.udp_858.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    