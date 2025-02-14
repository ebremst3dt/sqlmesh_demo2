
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'INTKUNDID': 'varchar(max)',
 'INTLEVID': 'varchar(max)',
 'KUND_PÅLOGG_FTG': 'varchar(max)',
 'LEVFAKTNR': 'varchar(max)',
 'LEV_PÅLOGG_FTG': 'varchar(max)',
 'MOTPKOMB': 'varchar(max)',
 'NR': 'varchar(max)'},
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
 		CAST(INTKUNDID AS VARCHAR(MAX)) AS intkundid,
		CAST(INTLEVID AS VARCHAR(MAX)) AS intlevid,
		CAST(KUND_PÅLOGG_FTG AS VARCHAR(MAX)) AS kund_pålogg_ftg,
		CAST(LEV_PÅLOGG_FTG AS VARCHAR(MAX)) AS lev_pålogg_ftg,
		CAST(LEVFAKTNR AS VARCHAR(MAX)) AS levfaktnr,
		CAST(MOTPKOMB AS VARCHAR(MAX)) AS motpkomb,
		CAST(NR AS VARCHAR(MAX)) AS nr 
	FROM utdata.utdata295.RK_DIM_INTKUNDFAKT_MOTP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
