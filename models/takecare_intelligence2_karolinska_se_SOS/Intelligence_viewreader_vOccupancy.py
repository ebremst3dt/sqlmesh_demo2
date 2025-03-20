
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Historik för beläggning (skapade beläggningsrapporter). Genereras med jämna mellanrum i TakeCare utifrån PAS-posterna.",
    columns={'_data_modified_utc': 'datetime', '_metadata_modified_utc': 'datetime', 'Admitted': 'varchar(max)', 'AppointedBeds': 'varchar(max)', 'AvailableBeds': 'varchar(max)', 'Beds': 'varchar(max)', 'CareUnitID': 'varchar(max)', 'Comment': 'varchar(max)', 'ConfirmedByUserName': 'varchar(max)', 'ConfirmedTime': 'varchar(max)', 'DischargeReady': 'varchar(max)', 'IsOpenFiveDaysAWeek': 'varchar(max)', 'OccupancyRate': 'varchar(max)', 'OnLeave': 'varchar(max)', 'Placed': 'varchar(max)', 'PlannedAdmissions': 'varchar(max)', 'PlannedDischarges': 'varchar(max)', 'ReportDate': 'varchar(max)', 'ReportTime': 'varchar(max)', 'TimestampRead': 'varchar(max)'},
    column_descriptions={'ReportDate': "{'title_ui': 'Datum', 'description': 'Datum för beläggningsdata'}", 'ReportTime': "{'title_ui': None, 'description': 'Klockslag då beläggningsdata-batch körts'}", 'CareUnitID': "{'title_ui': None, 'description': 'Vårdenhet som beläggningsdata gäller'}", 'AppointedBeds': "{'title_ui': 'Fastställda vårdplatser', 'description': 'Antal, enligt organisation'}", 'Beds': "{'title_ui': 'Disponibla vårdplatser', 'description': 'Antal, enligt vårdenhet'}", 'IsOpenFiveDaysAWeek': "{'title_ui': None, 'description': 'Om vårdenheten är av typen 5-dygns-vård. Visas i kommentarsfältet i TakeCare.'}", 'Admitted': "{'title_ui': 'Antal inskrivna', 'description': 'Antal inskrivna patienter som är placerade på denna vårdenhet'}", 'OnLeave': "{'title_ui': 'Antal permissioner', 'description': 'Antal patienter på permission'}", 'Placed': "{'title_ui': 'Antal utplacerade patienter', 'description': 'Antal patienter som utplacerats på andra vårdenheter'}", 'AvailableBeds': "{'title_ui': 'Lediga vårdplatser', 'description': 'Antal disponibla vårdplatser minus antal inskrivna patienter som är placerade på denna vårdenhet, plus antal patienter på permission'}", 'DischargeReady': "{'title_ui': 'MFB/Utskrivningsklara', 'description': None}", 'PlannedAdmissions': "{'title_ui': 'Planerade inskrivningar', 'description': None}", 'PlannedDischarges': "{'title_ui': 'Planerade utskrivningar', 'description': None}", 'OccupancyRate': "{'title_ui': 'Beläggning i %', 'description': None}", 'ConfirmedTime': "{'title_ui': 'Bekräftad', 'description': 'Klockslag för bekräftelse'}", 'ConfirmedByUserName': "{'title_ui': 'Bekräftad', 'description': 'Användarnamn på den som bekräftat'}", 'Comment': "{'title_ui': 'Kommentar', 'description': 'Vissa strängar läggs till av TakeCare i gränssnittet och lagras ej.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		CAST(Admitted AS VARCHAR(MAX)) AS Admitted,
		CAST(AppointedBeds AS VARCHAR(MAX)) AS AppointedBeds,
		CAST(AvailableBeds AS VARCHAR(MAX)) AS AvailableBeds,
		CAST(Beds AS VARCHAR(MAX)) AS Beds,
		CAST(CareUnitID AS VARCHAR(MAX)) AS CareUnitID,
		CAST(Comment AS VARCHAR(MAX)) AS Comment,
		CAST(ConfirmedByUserName AS VARCHAR(MAX)) AS ConfirmedByUserName,
		CONVERT(varchar(max), ConfirmedTime, 126) AS ConfirmedTime,
		CAST(DischargeReady AS VARCHAR(MAX)) AS DischargeReady,
		CAST(IsOpenFiveDaysAWeek AS VARCHAR(MAX)) AS IsOpenFiveDaysAWeek,
		CAST(OccupancyRate AS VARCHAR(MAX)) AS OccupancyRate,
		CAST(OnLeave AS VARCHAR(MAX)) AS OnLeave,
		CAST(Placed AS VARCHAR(MAX)) AS Placed,
		CAST(PlannedAdmissions AS VARCHAR(MAX)) AS PlannedAdmissions,
		CAST(PlannedDischarges AS VARCHAR(MAX)) AS PlannedDischarges,
		CONVERT(varchar(max), ReportDate, 126) AS ReportDate,
		CONVERT(varchar(max), ReportTime, 126) AS ReportTime,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead 
	FROM Intelligence.viewreader.vOccupancy) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence2.karolinska.se_SOS")
    