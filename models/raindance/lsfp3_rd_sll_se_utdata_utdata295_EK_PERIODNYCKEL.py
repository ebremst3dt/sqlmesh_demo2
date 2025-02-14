
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BASVARDE1': 'varchar(max)',
 'BASVARDE10': 'varchar(max)',
 'BASVARDE11': 'varchar(max)',
 'BASVARDE12': 'varchar(max)',
 'BASVARDE2': 'varchar(max)',
 'BASVARDE3': 'varchar(max)',
 'BASVARDE4': 'varchar(max)',
 'BASVARDE5': 'varchar(max)',
 'BASVARDE6': 'varchar(max)',
 'BASVARDE7': 'varchar(max)',
 'BASVARDE8': 'varchar(max)',
 'BASVARDE9': 'varchar(max)',
 'PNYCKEL': 'varchar(max)',
 'PNYCKEL_ID_TEXT': 'varchar(max)',
 'PNYCKEL_TEXT': 'varchar(max)',
 'TOTVARDE': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BASVARDE1 AS VARCHAR(MAX)) AS BASVARDE1,
CAST(BASVARDE10 AS VARCHAR(MAX)) AS BASVARDE10,
CAST(BASVARDE11 AS VARCHAR(MAX)) AS BASVARDE11,
CAST(BASVARDE12 AS VARCHAR(MAX)) AS BASVARDE12,
CAST(BASVARDE2 AS VARCHAR(MAX)) AS BASVARDE2,
CAST(BASVARDE3 AS VARCHAR(MAX)) AS BASVARDE3,
CAST(BASVARDE4 AS VARCHAR(MAX)) AS BASVARDE4,
CAST(BASVARDE5 AS VARCHAR(MAX)) AS BASVARDE5,
CAST(BASVARDE6 AS VARCHAR(MAX)) AS BASVARDE6,
CAST(BASVARDE7 AS VARCHAR(MAX)) AS BASVARDE7,
CAST(BASVARDE8 AS VARCHAR(MAX)) AS BASVARDE8,
CAST(BASVARDE9 AS VARCHAR(MAX)) AS BASVARDE9,
CAST(PNYCKEL AS VARCHAR(MAX)) AS PNYCKEL,
CAST(PNYCKEL_ID_TEXT AS VARCHAR(MAX)) AS PNYCKEL_ID_TEXT,
CAST(PNYCKEL_TEXT AS VARCHAR(MAX)) AS PNYCKEL_TEXT,
CAST(TOTVARDE AS VARCHAR(MAX)) AS TOTVARDE FROM utdata.utdata295.EK_PERIODNYCKEL"""
    return pipe(query=query)
