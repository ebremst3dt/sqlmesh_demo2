
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FRANGO_GILTIG_FOM': 'varchar(max)', 'FRANGO_GILTIG_TOM': 'varchar(max)', 'FRANGO_ID': 'varchar(max)', 'FRANGO_ID_TEXT': 'varchar(max)', 'FRANGO_PASSIV': 'varchar(max)', 'FRANGO_TEXT': 'varchar(max)', 'FRARAD_GILTIG_FOM': 'varchar(max)', 'FRARAD_GILTIG_TOM': 'varchar(max)', 'FRARAD_ID': 'varchar(max)', 'FRARAD_ID_TEXT': 'varchar(max)', 'FRARAD_PASSIV': 'varchar(max)', 'FRARAD_TEXT': 'varchar(max)', 'FRASUM_GILTIG_FOM': 'varchar(max)', 'FRASUM_GILTIG_TOM': 'varchar(max)', 'FRASUM_ID': 'varchar(max)', 'FRASUM_ID_TEXT': 'varchar(max)', 'FRASUM_PASSIV': 'varchar(max)', 'FRASUM_TEXT': 'varchar(max)', 'KKLASS_GILTIG_FOM': 'varchar(max)', 'KKLASS_GILTIG_TOM': 'varchar(max)', 'KKLASS_ID': 'varchar(max)', 'KKLASS_ID_TEXT': 'varchar(max)', 'KKLASS_PASSIV': 'varchar(max)', 'KKLASS_TEXT': 'varchar(max)', 'KTOGRP_GILTIG_FOM': 'varchar(max)', 'KTOGRP_GILTIG_TOM': 'varchar(max)', 'KTOGRP_ID': 'varchar(max)', 'KTOGRP_ID_TEXT': 'varchar(max)', 'KTOGRP_PASSIV': 'varchar(max)', 'KTOGRP_TEXT': 'varchar(max)', 'KTO_GILTIG_FOM': 'varchar(max)', 'KTO_GILTIG_TOM': 'varchar(max)', 'KTO_ID': 'varchar(max)', 'KTO_ID_TEXT': 'varchar(max)', 'KTO_PASSIV': 'varchar(max)', 'KTO_TEXT': 'varchar(max)', 'NIVÅ1_GILTIG_FOM': 'varchar(max)', 'NIVÅ1_GILTIG_TOM': 'varchar(max)', 'NIVÅ1_ID': 'varchar(max)', 'NIVÅ1_ID_TEXT': 'varchar(max)', 'NIVÅ1_PASSIV': 'varchar(max)', 'NIVÅ1_TEXT': 'varchar(max)', 'NIVÅ2_GILTIG_FOM': 'varchar(max)', 'NIVÅ2_GILTIG_TOM': 'varchar(max)', 'NIVÅ2_ID': 'varchar(max)', 'NIVÅ2_ID_TEXT': 'varchar(max)', 'NIVÅ2_PASSIV': 'varchar(max)', 'NIVÅ2_TEXT': 'varchar(max)', 'TABRAD_GILTIG_FOM': 'varchar(max)', 'TABRAD_GILTIG_TOM': 'varchar(max)', 'TABRAD_ID': 'varchar(max)', 'TABRAD_ID_TEXT': 'varchar(max)', 'TABRAD_PASSIV': 'varchar(max)', 'TABRAD_TEXT': 'varchar(max)'},
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
		'nksp_rd_sll_se_raindance_udp_udp_100' as _source,
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS frango_giltig_fom,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS frango_giltig_tom,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS frango_id,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS frango_id_text,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS frango_passiv,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS frango_text,
		CONVERT(varchar(max), FRARAD_GILTIG_FOM, 126) AS frarad_giltig_fom,
		CONVERT(varchar(max), FRARAD_GILTIG_TOM, 126) AS frarad_giltig_tom,
		CAST(FRARAD_ID AS VARCHAR(MAX)) AS frarad_id,
		CAST(FRARAD_ID_TEXT AS VARCHAR(MAX)) AS frarad_id_text,
		CAST(FRARAD_PASSIV AS VARCHAR(MAX)) AS frarad_passiv,
		CAST(FRARAD_TEXT AS VARCHAR(MAX)) AS frarad_text,
		CONVERT(varchar(max), FRASUM_GILTIG_FOM, 126) AS frasum_giltig_fom,
		CONVERT(varchar(max), FRASUM_GILTIG_TOM, 126) AS frasum_giltig_tom,
		CAST(FRASUM_ID AS VARCHAR(MAX)) AS frasum_id,
		CAST(FRASUM_ID_TEXT AS VARCHAR(MAX)) AS frasum_id_text,
		CAST(FRASUM_PASSIV AS VARCHAR(MAX)) AS frasum_passiv,
		CAST(FRASUM_TEXT AS VARCHAR(MAX)) AS frasum_text,
		CONVERT(varchar(max), KKLASS_GILTIG_FOM, 126) AS kklass_giltig_fom,
		CONVERT(varchar(max), KKLASS_GILTIG_TOM, 126) AS kklass_giltig_tom,
		CAST(KKLASS_ID AS VARCHAR(MAX)) AS kklass_id,
		CAST(KKLASS_ID_TEXT AS VARCHAR(MAX)) AS kklass_id_text,
		CAST(KKLASS_PASSIV AS VARCHAR(MAX)) AS kklass_passiv,
		CAST(KKLASS_TEXT AS VARCHAR(MAX)) AS kklass_text,
		CONVERT(varchar(max), KTOGRP_GILTIG_FOM, 126) AS ktogrp_giltig_fom,
		CONVERT(varchar(max), KTOGRP_GILTIG_TOM, 126) AS ktogrp_giltig_tom,
		CAST(KTOGRP_ID AS VARCHAR(MAX)) AS ktogrp_id,
		CAST(KTOGRP_ID_TEXT AS VARCHAR(MAX)) AS ktogrp_id_text,
		CAST(KTOGRP_PASSIV AS VARCHAR(MAX)) AS ktogrp_passiv,
		CAST(KTOGRP_TEXT AS VARCHAR(MAX)) AS ktogrp_text,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS kto_giltig_fom,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS kto_giltig_tom,
		CAST(KTO_ID AS VARCHAR(MAX)) AS kto_id,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS kto_id_text,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS kto_passiv,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS kto_text,
		CONVERT(varchar(max), NIVÅ1_GILTIG_FOM, 126) AS nivå1_giltig_fom,
		CONVERT(varchar(max), NIVÅ1_GILTIG_TOM, 126) AS nivå1_giltig_tom,
		CAST(NIVÅ1_ID AS VARCHAR(MAX)) AS nivå1_id,
		CAST(NIVÅ1_ID_TEXT AS VARCHAR(MAX)) AS nivå1_id_text,
		CAST(NIVÅ1_PASSIV AS VARCHAR(MAX)) AS nivå1_passiv,
		CAST(NIVÅ1_TEXT AS VARCHAR(MAX)) AS nivå1_text,
		CONVERT(varchar(max), NIVÅ2_GILTIG_FOM, 126) AS nivå2_giltig_fom,
		CONVERT(varchar(max), NIVÅ2_GILTIG_TOM, 126) AS nivå2_giltig_tom,
		CAST(NIVÅ2_ID AS VARCHAR(MAX)) AS nivå2_id,
		CAST(NIVÅ2_ID_TEXT AS VARCHAR(MAX)) AS nivå2_id_text,
		CAST(NIVÅ2_PASSIV AS VARCHAR(MAX)) AS nivå2_passiv,
		CAST(NIVÅ2_TEXT AS VARCHAR(MAX)) AS nivå2_text,
		CONVERT(varchar(max), TABRAD_GILTIG_FOM, 126) AS tabrad_giltig_fom,
		CONVERT(varchar(max), TABRAD_GILTIG_TOM, 126) AS tabrad_giltig_tom,
		CAST(TABRAD_ID AS VARCHAR(MAX)) AS tabrad_id,
		CAST(TABRAD_ID_TEXT AS VARCHAR(MAX)) AS tabrad_id_text,
		CAST(TABRAD_PASSIV AS VARCHAR(MAX)) AS tabrad_passiv,
		CAST(TABRAD_TEXT AS VARCHAR(MAX)) AS tabrad_text 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    