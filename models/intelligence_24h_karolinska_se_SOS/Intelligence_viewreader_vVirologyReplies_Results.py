
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Resultat från analyser för virologi. Ett svar kan innehålla flera resultat med ett eget utlåtande och svarsdatum. Ett resultat kan omfatta flera analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DocumentID': 'varchar(max)', 'LID': 'varchar(max)', 'LabNotes': 'varchar(max)', 'LabResponsibleDoctor': 'varchar(max)', 'PatientID': 'varchar(max)', 'ReplyType': 'varchar(max)', 'ResultComment': 'varchar(max)', 'ResultText': 'varchar(max)', 'ResultTimestamp': 'varchar(max)', 'Row': 'varchar(max)', 'SectionNotes': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'LID': "{'title_ui': 'L:', 'description': 'LID (labb-id), dvs. labbsvarets id i laboratoriets system'}", 'ResultComment': "{'title_ui': 'Noteringar/Upplysningar', 'description': 'Kommentar från labbet angående analysen/provet.'}", 'ResultText': "{'title_ui': 'Utlåtande', 'description': 'Svarstext'}", 'LabResponsibleDoctor': "{'title_ui': 'Läkare', 'description': 'Ansvarig läkare lab'}", 'ResultTimestamp': "{'title_ui': 'Svarsdatum', 'description': 'Tidpunkt då svar skickades'}", 'ReplyType': "{'title_ui': 'Typ av svar', 'description': 'Lagras och visas i klartext'}", 'SectionNotes': "{'title_ui': None, 'description': 'Generell information från aktuell labbsektion. Oftast samma för alla svar.'}", 'LabNotes': "{'title_ui': None, 'description': 'Generell information från labbet. Oftast samma för alla svar.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
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
		'intelligence_24h_karolinska_se_Intelligence_viewreader' as _source,
		CAST(DocumentID AS VARCHAR(MAX)) AS DocumentID,
		CAST(LID AS VARCHAR(MAX)) AS LID,
		CAST(LabNotes AS VARCHAR(MAX)) AS LabNotes,
		CAST(LabResponsibleDoctor AS VARCHAR(MAX)) AS LabResponsibleDoctor,
		CAST(PatientID AS VARCHAR(MAX)) AS PatientID,
		CAST(ReplyType AS VARCHAR(MAX)) AS ReplyType,
		CAST(ResultComment AS VARCHAR(MAX)) AS ResultComment,
		CAST(ResultText AS VARCHAR(MAX)) AS ResultText,
		CONVERT(varchar(max), ResultTimestamp, 126) AS ResultTimestamp,
		CAST(Row AS VARCHAR(MAX)) AS Row,
		CAST(SectionNotes AS VARCHAR(MAX)) AS SectionNotes,
		CONVERT(varchar(max), TimestampRead, 126) AS TimestampRead,
		CAST(Version AS VARCHAR(MAX)) AS Version 
	FROM Intelligence.viewreader.vVirologyReplies_Results) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_SOS")
    