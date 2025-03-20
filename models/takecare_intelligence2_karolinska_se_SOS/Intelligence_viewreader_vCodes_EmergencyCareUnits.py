
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="De vårdenheter som kan använda akutliggaren och skapa akutuppgifter.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'AmbulanceRecordID': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'HasCareTeams': 'varchar(max)', 'HospitalID': 'varchar(max)', 'IsActive': 'varchar(max)', 'ShortName': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'ValidThroughDate': 'varchar(max)'},
    column_descriptions={'CareUnitID': "{'title_ui': 'Id', 'description': 'Vårdenhet'}", 'ShortName': "{'title_ui': 'Kort namn', 'description': 'Det kortnamn som används för att presentera vårdenheter i akutliggaren.'}", 'HospitalID': "{'title_ui': 'Inrättning', 'description': 'En del av inrättningens kombika'}", 'AmbulanceRecordID': "{'title_ui': 'Ambulansjournalkod', 'description': 'Id för styrning av inläsning av ambulansjournaler'}", 'HasCareTeams': "{'title_ui': 'Vårdlag', 'description': 'Om vårdenhetan använder vårdlag'}", 'ValidThroughDate': "{'title_ui': 'Giltig t.o.m.', 'description': 'Sista datum som vårdenheten får använda akutliggaren.'}", 'IsActive': "{'title_ui': None, 'description': 'Om denna vårdenhet får använda akutliggaren för tillfället.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
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
 		CAST(CAST(TimestampRead AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'intelligence2_karolinska_se_Intelligence_viewreader' as _source,
		CAST(AmbulanceRecordID AS VARCHAR(MAX)) AS AmbulanceRecordID,
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CAST(HasCareTeams AS VARCHAR(MAX)) AS HasCareTeams,
		CAST(HospitalID AS VARCHAR(MAX)) AS HospitalID,
		CAST(IsActive AS VARCHAR(MAX)) AS IsActive,
		CAST(ShortName AS VARCHAR(MAX)) AS ShortName,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CONVERT(varchar(max), ValidThroughDate, 126) AS ValidThroughDate 
	FROM Intelligence.viewreader.vCodes_EmergencyCareUnits) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    