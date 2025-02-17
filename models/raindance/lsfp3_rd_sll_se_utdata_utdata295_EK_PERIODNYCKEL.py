
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BASVARDE1': 'numeric',
 'BASVARDE10': 'numeric',
 'BASVARDE11': 'numeric',
 'BASVARDE12': 'numeric',
 'BASVARDE2': 'numeric',
 'BASVARDE3': 'numeric',
 'BASVARDE4': 'numeric',
 'BASVARDE5': 'numeric',
 'BASVARDE6': 'numeric',
 'BASVARDE7': 'numeric',
 'BASVARDE8': 'numeric',
 'BASVARDE9': 'numeric',
 'PNYCKEL': 'varchar(6)',
 'PNYCKEL_ID_TEXT': 'varchar(38)',
 'PNYCKEL_TEXT': 'varchar(20)',
 'TOTVARDE': 'numeric'},
    kind=ModelKindName.FULL,
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
 		CAST(BASVARDE1 AS VARCHAR(MAX)) AS basvarde1,
		CAST(BASVARDE10 AS VARCHAR(MAX)) AS basvarde10,
		CAST(BASVARDE11 AS VARCHAR(MAX)) AS basvarde11,
		CAST(BASVARDE12 AS VARCHAR(MAX)) AS basvarde12,
		CAST(BASVARDE2 AS VARCHAR(MAX)) AS basvarde2,
		CAST(BASVARDE3 AS VARCHAR(MAX)) AS basvarde3,
		CAST(BASVARDE4 AS VARCHAR(MAX)) AS basvarde4,
		CAST(BASVARDE5 AS VARCHAR(MAX)) AS basvarde5,
		CAST(BASVARDE6 AS VARCHAR(MAX)) AS basvarde6,
		CAST(BASVARDE7 AS VARCHAR(MAX)) AS basvarde7,
		CAST(BASVARDE8 AS VARCHAR(MAX)) AS basvarde8,
		CAST(BASVARDE9 AS VARCHAR(MAX)) AS basvarde9,
		CAST(PNYCKEL AS VARCHAR(MAX)) AS pnyckel,
		CAST(PNYCKEL_ID_TEXT AS VARCHAR(MAX)) AS pnyckel_id_text,
		CAST(PNYCKEL_TEXT AS VARCHAR(MAX)) AS pnyckel_text,
		CAST(TOTVARDE AS VARCHAR(MAX)) AS totvarde 
	FROM utdata.utdata295.EK_PERIODNYCKEL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
