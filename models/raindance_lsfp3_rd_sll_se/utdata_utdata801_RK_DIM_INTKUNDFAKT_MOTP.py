
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
		'lsfp3_rd_sll_se_utdata_utdata801' as _source,
		CAST(INTKUNDID AS VARCHAR(MAX)) AS INTKUNDID,
		CAST(INTLEVID AS VARCHAR(MAX)) AS INTLEVID,
		CAST(KUND_PÅLOGG_FTG AS VARCHAR(MAX)) AS KUND_PÅLOGG_FTG,
		CAST(LEVFAKTNR AS VARCHAR(MAX)) AS LEVFAKTNR,
		CAST(LEV_PÅLOGG_FTG AS VARCHAR(MAX)) AS LEV_PÅLOGG_FTG,
		CAST(MOTPKOMB AS VARCHAR(MAX)) AS MOTPKOMB,
		CAST(NR AS VARCHAR(MAX)) AS NR 
	FROM utdata.utdata801.RK_DIM_INTKUNDFAKT_MOTP ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    