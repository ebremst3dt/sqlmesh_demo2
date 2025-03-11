
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'BOKBELOPP_INT': 'varchar(max)', 'BOKBELOPP_VAL': 'varchar(max)', 'BOKSTATUS': 'varchar(max)', 'BOKTYP': 'varchar(max)', 'DETALJTYP': 'varchar(max)', 'NR': 'varchar(max)', 'TAB_MOMS': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'VERNR': 'varchar(max)', 'VERRAD': 'varchar(max)'},
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
    query = """
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(BOKBELOPP_INT AS VARCHAR(MAX)) AS bokbelopp_int,
		CAST(BOKBELOPP_VAL AS VARCHAR(MAX)) AS bokbelopp_val,
		CAST(BOKSTATUS AS VARCHAR(MAX)) AS bokstatus,
		CAST(BOKTYP AS VARCHAR(MAX)) AS boktyp,
		CAST(DETALJTYP AS VARCHAR(MAX)) AS detaljtyp,
		CAST(NR AS VARCHAR(MAX)) AS nr,
		CAST(TAB_MOMS AS VARCHAR(MAX)) AS tab_moms,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERNR AS VARCHAR(MAX)) AS vernr,
		CAST(VERRAD AS VARCHAR(MAX)) AS verrad 
	FROM utdata.utdata295.RK_FAKTA_KUNDBOKDET) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    