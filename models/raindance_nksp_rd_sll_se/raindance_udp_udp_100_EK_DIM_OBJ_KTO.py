
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
		CONVERT(varchar(max), FRANGO_GILTIG_FOM, 126) AS FRANGO_GILTIG_FOM,
		CONVERT(varchar(max), FRANGO_GILTIG_TOM, 126) AS FRANGO_GILTIG_TOM,
		CAST(FRANGO_ID AS VARCHAR(MAX)) AS FRANGO_ID,
		CAST(FRANGO_ID_TEXT AS VARCHAR(MAX)) AS FRANGO_ID_TEXT,
		CAST(FRANGO_PASSIV AS VARCHAR(MAX)) AS FRANGO_PASSIV,
		CAST(FRANGO_TEXT AS VARCHAR(MAX)) AS FRANGO_TEXT,
		CONVERT(varchar(max), FRARAD_GILTIG_FOM, 126) AS FRARAD_GILTIG_FOM,
		CONVERT(varchar(max), FRARAD_GILTIG_TOM, 126) AS FRARAD_GILTIG_TOM,
		CAST(FRARAD_ID AS VARCHAR(MAX)) AS FRARAD_ID,
		CAST(FRARAD_ID_TEXT AS VARCHAR(MAX)) AS FRARAD_ID_TEXT,
		CAST(FRARAD_PASSIV AS VARCHAR(MAX)) AS FRARAD_PASSIV,
		CAST(FRARAD_TEXT AS VARCHAR(MAX)) AS FRARAD_TEXT,
		CONVERT(varchar(max), FRASUM_GILTIG_FOM, 126) AS FRASUM_GILTIG_FOM,
		CONVERT(varchar(max), FRASUM_GILTIG_TOM, 126) AS FRASUM_GILTIG_TOM,
		CAST(FRASUM_ID AS VARCHAR(MAX)) AS FRASUM_ID,
		CAST(FRASUM_ID_TEXT AS VARCHAR(MAX)) AS FRASUM_ID_TEXT,
		CAST(FRASUM_PASSIV AS VARCHAR(MAX)) AS FRASUM_PASSIV,
		CAST(FRASUM_TEXT AS VARCHAR(MAX)) AS FRASUM_TEXT,
		CONVERT(varchar(max), KKLASS_GILTIG_FOM, 126) AS KKLASS_GILTIG_FOM,
		CONVERT(varchar(max), KKLASS_GILTIG_TOM, 126) AS KKLASS_GILTIG_TOM,
		CAST(KKLASS_ID AS VARCHAR(MAX)) AS KKLASS_ID,
		CAST(KKLASS_ID_TEXT AS VARCHAR(MAX)) AS KKLASS_ID_TEXT,
		CAST(KKLASS_PASSIV AS VARCHAR(MAX)) AS KKLASS_PASSIV,
		CAST(KKLASS_TEXT AS VARCHAR(MAX)) AS KKLASS_TEXT,
		CONVERT(varchar(max), KTOGRP_GILTIG_FOM, 126) AS KTOGRP_GILTIG_FOM,
		CONVERT(varchar(max), KTOGRP_GILTIG_TOM, 126) AS KTOGRP_GILTIG_TOM,
		CAST(KTOGRP_ID AS VARCHAR(MAX)) AS KTOGRP_ID,
		CAST(KTOGRP_ID_TEXT AS VARCHAR(MAX)) AS KTOGRP_ID_TEXT,
		CAST(KTOGRP_PASSIV AS VARCHAR(MAX)) AS KTOGRP_PASSIV,
		CAST(KTOGRP_TEXT AS VARCHAR(MAX)) AS KTOGRP_TEXT,
		CONVERT(varchar(max), KTO_GILTIG_FOM, 126) AS KTO_GILTIG_FOM,
		CONVERT(varchar(max), KTO_GILTIG_TOM, 126) AS KTO_GILTIG_TOM,
		CAST(KTO_ID AS VARCHAR(MAX)) AS KTO_ID,
		CAST(KTO_ID_TEXT AS VARCHAR(MAX)) AS KTO_ID_TEXT,
		CAST(KTO_PASSIV AS VARCHAR(MAX)) AS KTO_PASSIV,
		CAST(KTO_TEXT AS VARCHAR(MAX)) AS KTO_TEXT,
		CONVERT(varchar(max), NIVÅ1_GILTIG_FOM, 126) AS NIVÅ1_GILTIG_FOM,
		CONVERT(varchar(max), NIVÅ1_GILTIG_TOM, 126) AS NIVÅ1_GILTIG_TOM,
		CAST(NIVÅ1_ID AS VARCHAR(MAX)) AS NIVÅ1_ID,
		CAST(NIVÅ1_ID_TEXT AS VARCHAR(MAX)) AS NIVÅ1_ID_TEXT,
		CAST(NIVÅ1_PASSIV AS VARCHAR(MAX)) AS NIVÅ1_PASSIV,
		CAST(NIVÅ1_TEXT AS VARCHAR(MAX)) AS NIVÅ1_TEXT,
		CONVERT(varchar(max), NIVÅ2_GILTIG_FOM, 126) AS NIVÅ2_GILTIG_FOM,
		CONVERT(varchar(max), NIVÅ2_GILTIG_TOM, 126) AS NIVÅ2_GILTIG_TOM,
		CAST(NIVÅ2_ID AS VARCHAR(MAX)) AS NIVÅ2_ID,
		CAST(NIVÅ2_ID_TEXT AS VARCHAR(MAX)) AS NIVÅ2_ID_TEXT,
		CAST(NIVÅ2_PASSIV AS VARCHAR(MAX)) AS NIVÅ2_PASSIV,
		CAST(NIVÅ2_TEXT AS VARCHAR(MAX)) AS NIVÅ2_TEXT,
		CONVERT(varchar(max), TABRAD_GILTIG_FOM, 126) AS TABRAD_GILTIG_FOM,
		CONVERT(varchar(max), TABRAD_GILTIG_TOM, 126) AS TABRAD_GILTIG_TOM,
		CAST(TABRAD_ID AS VARCHAR(MAX)) AS TABRAD_ID,
		CAST(TABRAD_ID_TEXT AS VARCHAR(MAX)) AS TABRAD_ID_TEXT,
		CAST(TABRAD_PASSIV AS VARCHAR(MAX)) AS TABRAD_PASSIV,
		CAST(TABRAD_TEXT AS VARCHAR(MAX)) AS TABRAD_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_KTO ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    