
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'INTKUNDID': 'varchar(max)', 'INTLEVID': 'varchar(max)', 'KUND_PÅLOGG_FTG': 'varchar(max)', 'LEVFAKTNR': 'varchar(max)', 'LEV_PÅLOGG_FTG': 'varchar(max)', 'MOTPKOMB': 'varchar(max)', 'NR': 'varchar(max)'},
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
		CAST(INTKUNDID AS VARCHAR(MAX)) AS intkundid,
		CAST(INTLEVID AS VARCHAR(MAX)) AS intlevid,
		CAST(KUND_PÅLOGG_FTG AS VARCHAR(MAX)) AS kund_pålogg_ftg,
		CAST(LEVFAKTNR AS VARCHAR(MAX)) AS levfaktnr,
		CAST(LEV_PÅLOGG_FTG AS VARCHAR(MAX)) AS lev_pålogg_ftg,
		CAST(MOTPKOMB AS VARCHAR(MAX)) AS motpkomb,
		CAST(NR AS VARCHAR(MAX)) AS nr 
	FROM stsudp.udp_858.RK_DIM_INTKUNDFAKT_MOTP ) y

	"""
    return read(query=query, server_url="stsp.rd.sll.se")
    