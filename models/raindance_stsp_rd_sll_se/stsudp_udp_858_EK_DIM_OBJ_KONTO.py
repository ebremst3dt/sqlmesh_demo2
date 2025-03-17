
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
		CONVERT(varchar(max), ANSELI_GILTIG_FOM, 126) AS anseli_giltig_fom,
		CONVERT(varchar(max), ANSELI_GILTIG_TOM, 126) AS anseli_giltig_tom,
		CAST(ANSELI_ID AS VARCHAR(MAX)) AS anseli_id,
		CAST(ANSELI_ID_TEXT AS VARCHAR(MAX)) AS anseli_id_text,
		CAST(ANSELI_PASSIV AS VARCHAR(MAX)) AS anseli_passiv,
		CAST(ANSELI_TEXT AS VARCHAR(MAX)) AS anseli_text,
		CONVERT(varchar(max), ELSJKH_GILTIG_FOM, 126) AS elsjkh_giltig_fom,
		CONVERT(varchar(max), ELSJKH_GILTIG_TOM, 126) AS elsjkh_giltig_tom,
		CAST(ELSJKH_ID AS VARCHAR(MAX)) AS elsjkh_id,
		CAST(ELSJKH_ID_TEXT AS VARCHAR(MAX)) AS elsjkh_id_text,
		CAST(ELSJKH_PASSIV AS VARCHAR(MAX)) AS elsjkh_passiv,
		CAST(ELSJKH_TEXT AS VARCHAR(MAX)) AS elsjkh_text,
		CONVERT(varchar(max), FRAGRP_GILTIG_FOM, 126) AS fragrp_giltig_fom,
		CONVERT(varchar(max), FRAGRP_GILTIG_TOM, 126) AS fragrp_giltig_tom,
		CAST(FRAGRP_ID AS VARCHAR(MAX)) AS fragrp_id,
		CAST(FRAGRP_ID_TEXT AS VARCHAR(MAX)) AS fragrp_id_text,
		CAST(FRAGRP_PASSIV AS VARCHAR(MAX)) AS fragrp_passiv,
		CAST(FRAGRP_TEXT AS VARCHAR(MAX)) AS fragrp_text,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS frango_giltig_fom,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS frango_giltig_tom,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS frango_id,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS frango_id_text,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS frango_passiv,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS frango_text,
		CONVERT(varchar(max), IK_GILTIG_FOM, 126) AS ik_giltig_fom,
		CONVERT(varchar(max), IK_GILTIG_TOM, 126) AS ik_giltig_tom,
		CAST(IK_ID AS VARCHAR(MAX)) AS ik_id,
		CAST(IK_ID_TEXT AS VARCHAR(MAX)) AS ik_id_text,
		CAST(IK_PASSIV AS VARCHAR(MAX)) AS ik_passiv,
		CAST(IK_TEXT AS VARCHAR(MAX)) AS ik_text,
		CONVERT(varchar(max), KGRUPP_GILTIG_FOM, 126) AS kgrupp_giltig_fom,
		CONVERT(varchar(max), KGRUPP_GILTIG_TOM, 126) AS kgrupp_giltig_tom,
		CAST(KGRUPP_ID AS VARCHAR(MAX)) AS kgrupp_id,
		CAST(KGRUPP_ID_TEXT AS VARCHAR(MAX)) AS kgrupp_id_text,
		CAST(KGRUPP_PASSIV AS VARCHAR(MAX)) AS kgrupp_passiv,
		CAST(KGRUPP_TEXT AS VARCHAR(MAX)) AS kgrupp_text,
		CONVERT(varchar(max), KKLASS_GILTIG_FOM, 126) AS kklass_giltig_fom,
		CONVERT(varchar(max), KKLASS_GILTIG_TOM, 126) AS kklass_giltig_tom,
		CAST(KKLASS_ID AS VARCHAR(MAX)) AS kklass_id,
		CAST(KKLASS_ID_TEXT AS VARCHAR(MAX)) AS kklass_id_text,
		CAST(KKLASS_PASSIV AS VARCHAR(MAX)) AS kklass_passiv,
		CAST(KKLASS_TEXT AS VARCHAR(MAX)) AS kklass_text,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS konto_giltig_fom,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS konto_giltig_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS konto_id_text,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS konto_passiv,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS konto_text,
		CONVERT(varchar(max), RAD_GILTIG_FOM, 126) AS rad_giltig_fom,
		CONVERT(varchar(max), RAD_GILTIG_TOM, 126) AS rad_giltig_tom,
		CAST(RAD_ID AS VARCHAR(MAX)) AS rad_id,
		CAST(RAD_ID_TEXT AS VARCHAR(MAX)) AS rad_id_text,
		CAST(RAD_PASSIV AS VARCHAR(MAX)) AS rad_passiv,
		CAST(RAD_TEXT AS VARCHAR(MAX)) AS rad_text,
		CONVERT(varchar(max), RR_STS_GILTIG_FOM, 126) AS rr_sts_giltig_fom,
		CONVERT(varchar(max), RR_STS_GILTIG_TOM, 126) AS rr_sts_giltig_tom,
		CAST(RR_STS_ID AS VARCHAR(MAX)) AS rr_sts_id,
		CAST(RR_STS_ID_TEXT AS VARCHAR(MAX)) AS rr_sts_id_text,
		CAST(RR_STS_PASSIV AS VARCHAR(MAX)) AS rr_sts_passiv,
		CAST(RR_STS_TEXT AS VARCHAR(MAX)) AS rr_sts_text,
		CONVERT(varchar(max), ÅRL_GILTIG_FOM, 126) AS årl_giltig_fom,
		CONVERT(varchar(max), ÅRL_GILTIG_TOM, 126) AS årl_giltig_tom,
		CAST(ÅRL_ID AS VARCHAR(MAX)) AS årl_id,
		CAST(ÅRL_ID_TEXT AS VARCHAR(MAX)) AS årl_id_text,
		CAST(ÅRL_PASSIV AS VARCHAR(MAX)) AS årl_passiv,
		CAST(ÅRL_TEXT AS VARCHAR(MAX)) AS årl_text 
	FROM stsudp.udp_858.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    