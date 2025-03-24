
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

    
@model(
    description="Resultat från analyser.",
    columns={'_data_modified_utc': 'datetime2', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'Analysis': 'varchar(max)', 'AnalysisComment': 'varchar(max)', 'AnalysisID': 'varchar(max)', 'AnalysisMessage': 'varchar(max)', 'DisciplineCode': 'varchar(max)', 'DisciplineLabel': 'varchar(max)', 'DocumentID': 'varchar(max)', 'Group': 'varchar(max)', 'GroupComment': 'varchar(max)', 'IsAccredited': 'varchar(max)', 'IsDeviating': 'varchar(max)', 'IsReplacingFinal': 'varchar(max)', 'LabInternalNo': 'varchar(max)', 'MachineTime': 'varchar(max)', 'MedicalApprovedSignature': 'varchar(max)', 'MedicallyResponsibleSignature': 'varchar(max)', 'PatientID': 'varchar(max)', 'RefMax': 'varchar(max)', 'RefMin': 'varchar(max)', 'RefOperator': 'varchar(max)', 'RefText': 'varchar(max)', 'ReferenceArea1': 'varchar(max)', 'ReferenceArea2': 'varchar(max)', 'ReferenceArea3': 'varchar(max)', 'ReferenceArea4': 'varchar(max)', 'ReplacementValue': 'varchar(max)', 'ReplyTimestamp': 'varchar(max)', 'ResultText': 'varchar(max)', 'ResultTextCode': 'varchar(max)', 'Row': 'varchar(max)', 'SampleID': 'varchar(max)', 'TechnicalApprovedSignature': 'varchar(max)', 'TestGroupCode': 'varchar(max)', 'TimestampRead': 'varchar(max)', 'Unit': 'varchar(max)', 'Value': 'varchar(max)', 'Version': 'varchar(max)'},
    column_descriptions={'PatientID': "{'title_ui': None, 'description': 'Patientens id (person- eller reservnummer)'}", 'DocumentID': "{'title_ui': None, 'description': 'Internt id som identifierar dokumentet i journalen'}", 'Version': "{'title_ui': None, 'description': 'Löpnummer för version av dokument. Kan förändras mellan körningar.'}", 'AnalysisID': "{'title_ui': None, 'description': 'Labbets kod för analysen'}", 'Row': "{'title_ui': None, 'description': 'Internt rad- eller löpnummer'}", 'ReplyTimestamp': "{'title_ui': None, 'description': 'Tiden då laboratoriet skickade svaret.'}", 'MachineTime': "{'title_ui': None, 'description': 'Tiden då labbsystemet skapade svaret.'}", 'Analysis': "{'title_ui': 'Analysnamn', 'description': 'Namn/beteckning för analys som utförts'}", 'ReplacementValue': '{\'title_ui\': None, \'description\': "Används när svaret på analysen inte matchar analysvärdesfältet. Kan vara t ex, ett intervall, \'Saknas\', \'Utförd\'"}', 'Value': "{'title_ui': 'Resultat', 'description': 'Värde på resultatet av labanalysen. Ibland en sträng, ibland ett tal.'}", 'Unit': "{'title_ui': 'Enhet', 'description': 'Enhet för resultatet'}", 'ReferenceArea1': "{'title_ui': 'Referensintervall', 'description': 'Referensintervall 1. Om RefOperatorIsActivated i huvudtabellen är satt så ska denna kolumn inte användas. Då ska ersättande kolumner användas.'}", 'ReferenceArea2': "{'title_ui': 'Referensintervall', 'description': 'Referensintervall 2 om det finns fler än 1. Se kommentaren till ReferenceArea1'}", 'ReferenceArea3': "{'title_ui': 'Referensintervall', 'description': 'Referensintervall 3 om det finns fler än 2. Se kommentaren till ReferenceArea1'}", 'ReferenceArea4': "{'title_ui': 'Referensintervall', 'description': 'Referensintervall 4 om det finns fler än 3. Se kommentaren till ReferenceArea1'}", 'IsDeviating': "{'title_ui': '*', 'description': 'Om värdet är utanför referensintervall'}", 'AnalysisComment': "{'title_ui': 'Analyskommentar', 'description': 'Kommentar till labanalysen'}", 'Group': "{'title_ui': None, 'description': 'De analyser som har samma nummer grupperas tillsammans i svaret.'}", 'GroupComment': "{'title_ui': 'Provkommentar', 'description': 'Kommentar till den grupp (det prov) som denna analys ligger i'}", 'ResultTextCode': "{'title_ui': None, 'description': 'Labbets interna kod för resultatexten'}", 'ResultText': "{'title_ui': 'Resultattext', 'description': None}", 'LabInternalNo': "{'title_ui': None, 'description': 'Labbets interna id på provet'}", 'TechnicalApprovedSignature': "{'title_ui': None, 'description': 'Signatur för tekniskt godkännande'}", 'MedicalApprovedSignature': "{'title_ui': None, 'description': 'Signatur för medicinskt godkännande'}", 'IsAccredited': "{'title_ui': '**Ackrediterad analys enligt Swedac', 'description': 'Om analysmetoden är ackrediterad'}", 'MedicallyResponsibleSignature': "{'title_ui': None, 'description': 'Signatur för medicinskt ansvarig'}", 'TestGroupCode': "{'title_ui': None, 'description': 'Testgruppkod om testremissen är i en testgrupp'}", 'IsReplacingFinal': "{'title_ui': None, 'description': 'Om detta är ett slutgiltigt resultat som ersätter ett tidigare skickat slutresultat'}", 'AnalysisMessage': "{'title_ui': 'Analyskommentar', 'description': 'Analysmeddelande'}", 'SampleID': "{'title_ui': None, 'description': 'Labbets id på provet'}", 'DisciplineCode': "{'title_ui': None, 'description': 'Disciplinkod'}", 'DisciplineLabel': "{'title_ui': None, 'description': 'Disciplinnamn'}", 'RefOperator': "{'title_ui': 'Referensintervall', 'description': 'Operator i referensintervall. Se kommentar till RefOperatorIsActivated i huvudtabellen.'}", 'RefMin': "{'title_ui': 'Referensintervall', 'description': 'Intervallets min. Se kommentar till RefOperatorIsActivated i huvudtabellen.'}", 'RefMax': "{'title_ui': 'Referensintervall', 'description': 'Intervallets max. Se kommentar till RefOperatorIsActivated i huvudtabellen.'}", 'RefText': "{'title_ui': 'Referensintervall', 'description': 'Referensintervall fritext. Om denna är satt ska min, max och operator ej användas. Se även kommentar till RefOperatorIsActivated i huvudtabellen.'}", 'TimestampRead': "{'title_ui': None, 'description': 'När data lästs in från TakeCare-databasen'}"},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,

        time_column="_data_modified_utc"
    ),
    cron="@daily",
    enabled=True
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
		CAST([Analysis] AS VARCHAR(MAX)) AS [Analysis],
		CAST([AnalysisComment] AS VARCHAR(MAX)) AS [AnalysisComment],
		CAST([AnalysisID] AS VARCHAR(MAX)) AS [AnalysisID],
		CAST([AnalysisMessage] AS VARCHAR(MAX)) AS [AnalysisMessage],
		CAST([DisciplineCode] AS VARCHAR(MAX)) AS [DisciplineCode],
		CAST([DisciplineLabel] AS VARCHAR(MAX)) AS [DisciplineLabel],
		CAST([DocumentID] AS VARCHAR(MAX)) AS [DocumentID],
		CAST([Group] AS VARCHAR(MAX)) AS [Group],
		CAST([GroupComment] AS VARCHAR(MAX)) AS [GroupComment],
		CAST([IsAccredited] AS VARCHAR(MAX)) AS [IsAccredited],
		CAST([IsDeviating] AS VARCHAR(MAX)) AS [IsDeviating],
		CAST([IsReplacingFinal] AS VARCHAR(MAX)) AS [IsReplacingFinal],
		CAST([LabInternalNo] AS VARCHAR(MAX)) AS [LabInternalNo],
		CONVERT(varchar(max), [MachineTime], 126) AS [MachineTime],
		CAST([MedicalApprovedSignature] AS VARCHAR(MAX)) AS [MedicalApprovedSignature],
		CAST([MedicallyResponsibleSignature] AS VARCHAR(MAX)) AS [MedicallyResponsibleSignature],
		CAST([PatientID] AS VARCHAR(MAX)) AS [PatientID],
		CAST([RefMax] AS VARCHAR(MAX)) AS [RefMax],
		CAST([RefMin] AS VARCHAR(MAX)) AS [RefMin],
		CAST([RefOperator] AS VARCHAR(MAX)) AS [RefOperator],
		CAST([RefText] AS VARCHAR(MAX)) AS [RefText],
		CAST([ReferenceArea1] AS VARCHAR(MAX)) AS [ReferenceArea1],
		CAST([ReferenceArea2] AS VARCHAR(MAX)) AS [ReferenceArea2],
		CAST([ReferenceArea3] AS VARCHAR(MAX)) AS [ReferenceArea3],
		CAST([ReferenceArea4] AS VARCHAR(MAX)) AS [ReferenceArea4],
		CAST([ReplacementValue] AS VARCHAR(MAX)) AS [ReplacementValue],
		CONVERT(varchar(max), [ReplyTimestamp], 126) AS [ReplyTimestamp],
		CAST([ResultText] AS VARCHAR(MAX)) AS [ResultText],
		CAST([ResultTextCode] AS VARCHAR(MAX)) AS [ResultTextCode],
		CAST([Row] AS VARCHAR(MAX)) AS [Row],
		CAST([SampleID] AS VARCHAR(MAX)) AS [SampleID],
		CAST([TechnicalApprovedSignature] AS VARCHAR(MAX)) AS [TechnicalApprovedSignature],
		CAST([TestGroupCode] AS VARCHAR(MAX)) AS [TestGroupCode],
		CONVERT(varchar(max), [TimestampRead], 126) AS [TimestampRead],
		CAST([Unit] AS VARCHAR(MAX)) AS [Unit],
		CAST([Value] AS VARCHAR(MAX)) AS [Value],
		CAST([Version] AS VARCHAR(MAX)) AS [Version] 
	FROM Intelligence.viewreader.vMultiLabReplies_Analyses) y
	WHERE _data_modified_utc between '{start}' and '{end}'
	"""
    return read(query=query, server_url="intelligence_24h.karolinska.se_STS")
    