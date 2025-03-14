
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANTECKNING': 'varchar(max)', 'ANTECKNING1': 'varchar(max)', 'ANTECKNING2': 'varchar(max)', 'ANTECKNING3': 'varchar(max)', 'ANTECKNING4': 'varchar(max)', 'ANTECKNING5': 'varchar(max)', 'ANTECKNING6': 'varchar(max)', 'ANTECKNING7': 'varchar(max)', 'ANTECKNING8': 'varchar(max)', 'ANTECKNING9': 'varchar(max)', 'ANTECKNING10': 'varchar(max)', 'DOKTYP': 'varchar(max)', 'DOKUMENTID': 'varchar(max)'},
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
		CAST(ANTECKNING AS VARCHAR(MAX)) AS anteckning,
		CAST(ANTECKNING1 AS VARCHAR(MAX)) AS anteckning1,
		CAST(ANTECKNING2 AS VARCHAR(MAX)) AS anteckning2,
		CAST(ANTECKNING3 AS VARCHAR(MAX)) AS anteckning3,
		CAST(ANTECKNING4 AS VARCHAR(MAX)) AS anteckning4,
		CAST(ANTECKNING5 AS VARCHAR(MAX)) AS anteckning5,
		CAST(ANTECKNING6 AS VARCHAR(MAX)) AS anteckning6,
		CAST(ANTECKNING7 AS VARCHAR(MAX)) AS anteckning7,
		CAST(ANTECKNING8 AS VARCHAR(MAX)) AS anteckning8,
		CAST(ANTECKNING9 AS VARCHAR(MAX)) AS anteckning9,
		CAST(ANTECKNING10 AS VARCHAR(MAX)) AS anteckning10,
		CAST(DOKTYP AS VARCHAR(MAX)) AS doktyp,
		CAST(DOKUMENTID AS VARCHAR(MAX)) AS dokumentid 
	FROM utdata.utdata295.EK_ANTECKNING) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    