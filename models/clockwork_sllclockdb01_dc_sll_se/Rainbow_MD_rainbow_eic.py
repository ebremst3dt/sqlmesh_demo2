
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'adcsf1': 'varchar(max)', 'adcsf2': 'varchar(max)', 'adcst1': 'varchar(max)', 'adcst2': 'varchar(max)', 'adcsv1': 'varchar(max)', 'adcsv2': 'varchar(max)', 'adddsc': 'varchar(max)', 'ageval': 'varchar(max)', 'agporp': 'varchar(max)', 'agrnum': 'varchar(max)', 'agrqty': 'varchar(max)', 'agrusd': 'varchar(max)', 'agvunt': 'varchar(max)', 'altnum': 'varchar(max)', 'cattyp': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'cntori': 'varchar(max)', 'compny': 'varchar(max)', 'cprmut': 'varchar(max)', 'cprmuv': 'varchar(max)', 'cptwor': 'varchar(max)', 'cpypor': 'varchar(max)', 'cpytxt': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'curcod': 'varchar(max)', 'curprc': 'varchar(max)', 'dldact': 'varchar(max)', 'dlddat': 'varchar(max)', 'dldmax': 'varchar(max)', 'dldmin': 'varchar(max)', 'dlvday': 'varchar(max)', 'dppmpc': 'varchar(max)', 'dscmtx': 'varchar(max)', 'eicact': 'varchar(max)', 'eiclcs': 'varchar(max)', 'eictxt': 'varchar(max)', 'envcls': 'varchar(max)', 'extcod': 'varchar(max)', 'extdrw': 'varchar(max)', 'extict': 'varchar(max)', 'extigr': 'varchar(max)', 'extitm': 'varchar(max)', 'extity': 'varchar(max)', 'extnam': 'varchar(max)', 'extrra': 'varchar(max)', 'extrty': 'varchar(max)', 'extsys': 'varchar(max)', 'exttyp': 'varchar(max)', 'extunt': 'varchar(max)', 'exwldt': 'varchar(max)', 'grsam2': 'varchar(max)', 'grsvdm': 'varchar(max)', 'grswkg': 'varchar(max)', 'hidsrc': 'varchar(max)', 'inthnd': 'varchar(max)', 'itkseq': 'varchar(max)', 'itmcod': 'varchar(max)', 'lcsdat': 'varchar(max)', 'lcsusr': 'varchar(max)', 'maxddd': 'varchar(max)', 'maxqty': 'varchar(max)', 'menddd': 'varchar(max)', 'minddd': 'varchar(max)', 'minqty': 'varchar(max)', 'minunt': 'varchar(max)', 'mrkldt': 'varchar(max)', 'mtxgrp': 'varchar(max)', 'netam2': 'varchar(max)', 'netvdm': 'varchar(max)', 'netwkg': 'varchar(max)', 'newprc': 'varchar(max)', 'owncod': 'varchar(max)', 'owntyp': 'varchar(max)', 'pdcitc': 'varchar(max)', 'pdcitn': 'varchar(max)', 'pdcnam': 'varchar(max)', 'prcdat': 'varchar(max)', 'prcmtx': 'varchar(max)', 'prcunt': 'varchar(max)', 'prinum': 'varchar(max)', 'ptdlvm': 'varchar(max)', 'recycl': 'varchar(max)', 'revnum': 'varchar(max)', 'seqnum': 'varchar(max)', 'tdlcod': 'varchar(max)', 'tpycod': 'varchar(max)', 'untinf': 'varchar(max)', 'usefco': 'varchar(max)', 'valfrm': 'varchar(max)', 'wtrcod': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        batch_size=5000,
        time_column="_data_modified_utc"
    ),
    cron="@daily",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_Rainbow_MD_rainbow_eic_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.Rainbow_MD_rainbow_eic (_data_modified_utc)"]
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
 		CAST(
        CAST(
            COALESCE(
                CASE
                    WHEN credat > chgdat or chgdat IS NULL then credat
                    WHEN chgdat > credat or credat is NULL then chgdat
                    ELSE credat
                END,
                chgdat,
                credat
            ) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC'
        AS datetime2
    ) AS DATE ) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'Rainbow_MD' as _source_catalog,
		CAST(adcsf1 AS VARCHAR(MAX)) AS adcsf1,
		CAST(adcsf2 AS VARCHAR(MAX)) AS adcsf2,
		CAST(adcst1 AS VARCHAR(MAX)) AS adcst1,
		CAST(adcst2 AS VARCHAR(MAX)) AS adcst2,
		CAST(adcsv1 AS VARCHAR(MAX)) AS adcsv1,
		CAST(adcsv2 AS VARCHAR(MAX)) AS adcsv2,
		CAST(adddsc AS VARCHAR(MAX)) AS adddsc,
		CONVERT(varchar(max), ageval, 126) AS ageval,
		CAST(agporp AS VARCHAR(MAX)) AS agporp,
		CAST(agrnum AS VARCHAR(MAX)) AS agrnum,
		CAST(agrqty AS VARCHAR(MAX)) AS agrqty,
		CAST(agrusd AS VARCHAR(MAX)) AS agrusd,
		CONVERT(varchar(max), agvunt, 126) AS agvunt,
		CAST(altnum AS VARCHAR(MAX)) AS altnum,
		CAST(cattyp AS VARCHAR(MAX)) AS cattyp,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CAST(cntori AS VARCHAR(MAX)) AS cntori,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CAST(cprmut AS VARCHAR(MAX)) AS cprmut,
		CAST(cprmuv AS VARCHAR(MAX)) AS cprmuv,
		CAST(cptwor AS VARCHAR(MAX)) AS cptwor,
		CAST(cpypor AS VARCHAR(MAX)) AS cpypor,
		CAST(cpytxt AS VARCHAR(MAX)) AS cpytxt,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(curcod AS VARCHAR(MAX)) AS curcod,
		CAST(curprc AS VARCHAR(MAX)) AS curprc,
		CAST(dldact AS VARCHAR(MAX)) AS dldact,
		CONVERT(varchar(max), dlddat, 126) AS dlddat,
		CAST(dldmax AS VARCHAR(MAX)) AS dldmax,
		CAST(dldmin AS VARCHAR(MAX)) AS dldmin,
		CAST(dlvday AS VARCHAR(MAX)) AS dlvday,
		CAST(dppmpc AS VARCHAR(MAX)) AS dppmpc,
		CAST(dscmtx AS VARCHAR(MAX)) AS dscmtx,
		CAST(eicact AS VARCHAR(MAX)) AS eicact,
		CAST(eiclcs AS VARCHAR(MAX)) AS eiclcs,
		CAST(eictxt AS VARCHAR(MAX)) AS eictxt,
		CAST(envcls AS VARCHAR(MAX)) AS envcls,
		CAST(extcod AS VARCHAR(MAX)) AS extcod,
		CAST(extdrw AS VARCHAR(MAX)) AS extdrw,
		CAST(extict AS VARCHAR(MAX)) AS extict,
		CAST(extigr AS VARCHAR(MAX)) AS extigr,
		CAST(extitm AS VARCHAR(MAX)) AS extitm,
		CAST(extity AS VARCHAR(MAX)) AS extity,
		CAST(extnam AS VARCHAR(MAX)) AS extnam,
		CAST(extrra AS VARCHAR(MAX)) AS extrra,
		CAST(extrty AS VARCHAR(MAX)) AS extrty,
		CAST(extsys AS VARCHAR(MAX)) AS extsys,
		CAST(exttyp AS VARCHAR(MAX)) AS exttyp,
		CAST(extunt AS VARCHAR(MAX)) AS extunt,
		CAST(exwldt AS VARCHAR(MAX)) AS exwldt,
		CAST(grsam2 AS VARCHAR(MAX)) AS grsam2,
		CAST(grsvdm AS VARCHAR(MAX)) AS grsvdm,
		CAST(grswkg AS VARCHAR(MAX)) AS grswkg,
		CAST(hidsrc AS VARCHAR(MAX)) AS hidsrc,
		CAST(inthnd AS VARCHAR(MAX)) AS inthnd,
		CAST(itkseq AS VARCHAR(MAX)) AS itkseq,
		CAST(itmcod AS VARCHAR(MAX)) AS itmcod,
		CONVERT(varchar(max), lcsdat, 126) AS lcsdat,
		CAST(lcsusr AS VARCHAR(MAX)) AS lcsusr,
		CAST(maxddd AS VARCHAR(MAX)) AS maxddd,
		CAST(maxqty AS VARCHAR(MAX)) AS maxqty,
		CAST(menddd AS VARCHAR(MAX)) AS menddd,
		CAST(minddd AS VARCHAR(MAX)) AS minddd,
		CAST(minqty AS VARCHAR(MAX)) AS minqty,
		CAST(minunt AS VARCHAR(MAX)) AS minunt,
		CAST(mrkldt AS VARCHAR(MAX)) AS mrkldt,
		CAST(mtxgrp AS VARCHAR(MAX)) AS mtxgrp,
		CAST(netam2 AS VARCHAR(MAX)) AS netam2,
		CAST(netvdm AS VARCHAR(MAX)) AS netvdm,
		CAST(netwkg AS VARCHAR(MAX)) AS netwkg,
		CAST(newprc AS VARCHAR(MAX)) AS newprc,
		CAST(owncod AS VARCHAR(MAX)) AS owncod,
		CAST(owntyp AS VARCHAR(MAX)) AS owntyp,
		CAST(pdcitc AS VARCHAR(MAX)) AS pdcitc,
		CAST(pdcitn AS VARCHAR(MAX)) AS pdcitn,
		CAST(pdcnam AS VARCHAR(MAX)) AS pdcnam,
		CONVERT(varchar(max), prcdat, 126) AS prcdat,
		CAST(prcmtx AS VARCHAR(MAX)) AS prcmtx,
		CAST(prcunt AS VARCHAR(MAX)) AS prcunt,
		CAST(prinum AS VARCHAR(MAX)) AS prinum,
		CAST(ptdlvm AS VARCHAR(MAX)) AS ptdlvm,
		CAST(recycl AS VARCHAR(MAX)) AS recycl,
		CAST(revnum AS VARCHAR(MAX)) AS revnum,
		CAST(seqnum AS VARCHAR(MAX)) AS seqnum,
		CAST(tdlcod AS VARCHAR(MAX)) AS tdlcod,
		CAST(tpycod AS VARCHAR(MAX)) AS tpycod,
		CAST(untinf AS VARCHAR(MAX)) AS untinf,
		CAST(usefco AS VARCHAR(MAX)) AS usefco,
		CONVERT(varchar(max), valfrm, 126) AS valfrm,
		CAST(wtrcod AS VARCHAR(MAX)) AS wtrcod 
	FROM Rainbow_MD.rainbow.eic
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    