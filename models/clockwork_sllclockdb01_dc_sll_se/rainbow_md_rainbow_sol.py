
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'actcst': 'varchar(max)', 'actdat': 'varchar(max)', 'adsseq': 'varchar(max)', 'agncod': 'varchar(max)', 'agrnum': 'varchar(max)', 'am2typ': 'varchar(max)', 'bodseq': 'varchar(max)', 'cbchld': 'varchar(max)', 'cbycod': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgusr': 'varchar(max)', 'cnfdsp': 'varchar(max)', 'cnfrcv': 'varchar(max)', 'cntori': 'varchar(max)', 'compny': 'varchar(max)', 'credat': 'varchar(max)', 'creref': 'varchar(max)', 'creseq': 'varchar(max)', 'creusr': 'varchar(max)', 'csccod': 'varchar(max)', 'cstcod': 'varchar(max)', 'cstprc': 'varchar(max)', 'ctfseq': 'varchar(max)', 'cwsbtr': 'varchar(max)', 'dfiseq': 'varchar(max)', 'dlvdat': 'varchar(max)', 'dlvmrk': 'varchar(max)', 'dptcod': 'varchar(max)', 'dspdat': 'varchar(max)', 'earcod': 'varchar(max)', 'edists': 'varchar(max)', 'entfrm': 'varchar(max)', 'estcst': 'varchar(max)', 'etocod': 'varchar(max)', 'excexd': 'varchar(max)', 'excidn': 'varchar(max)', 'excivd': 'varchar(max)', 'exctrd': 'varchar(max)', 'expdat': 'varchar(max)', 'extcod': 'varchar(max)', 'extitm': 'varchar(max)', 'extnam': 'varchar(max)', 'extseq': 'varchar(max)', 'grsam2': 'varchar(max)', 'grsvdm': 'varchar(max)', 'grswkg': 'varchar(max)', 'icscat': 'varchar(max)', 'icsref': 'varchar(max)', 'idfent': 'varchar(max)', 'idncod': 'varchar(max)', 'idnsel': 'varchar(max)', 'invnow': 'varchar(max)', 'ipiadd': 'varchar(max)', 'itmadd': 'varchar(max)', 'kepcst': 'varchar(max)', 'kitseq': 'varchar(max)', 'lincod': 'varchar(max)', 'linnam': 'varchar(max)', 'linnum': 'varchar(max)', 'lintyp': 'varchar(max)', 'lstdat': 'varchar(max)', 'lststs': 'varchar(max)', 'manddn': 'varchar(max)', 'manddt': 'varchar(max)', 'manddv': 'varchar(max)', 'mstads': 'varchar(max)', 'mstctf': 'varchar(max)', 'mstkit': 'varchar(max)', 'netam2': 'varchar(max)', 'netvdm': 'varchar(max)', 'netwkg': 'varchar(max)', 'numpal': 'varchar(max)', 'numpcl': 'varchar(max)', 'ofmcod': 'varchar(max)', 'ofmreq': 'varchar(max)', 'onhold': 'varchar(max)', 'orgitm': 'varchar(max)', 'ownadd': 'varchar(max)', 'owndat': 'varchar(max)', 'pckdat': 'varchar(max)', 'posnum': 'varchar(max)', 'prcact': 'varchar(max)', 'prcbas': 'varchar(max)', 'prccst': 'varchar(max)', 'prcdim': 'varchar(max)', 'prcgrs': 'varchar(max)', 'prcmth': 'varchar(max)', 'prcrsv': 'varchar(max)', 'prdcst': 'varchar(max)', 'prdrsv': 'varchar(max)', 'prjcod': 'varchar(max)', 'prttxg': 'varchar(max)', 'prttxi': 'varchar(max)', 'q2barr': 'varchar(max)', 'q2basm': 'varchar(max)', 'q2bcnf': 'varchar(max)', 'q2bdlv': 'varchar(max)', 'q2bext': 'varchar(max)', 'q2binv': 'varchar(max)', 'q2bpck': 'varchar(max)', 'q2bpkd': 'varchar(max)', 'q2bprc': 'varchar(max)', 'q2bprd': 'varchar(max)', 'q2brcv': 'varchar(max)', 'q2bret': 'varchar(max)', 'q2brsv': 'varchar(max)', 'qtyarr': 'varchar(max)', 'qtyasm': 'varchar(max)', 'qtyava': 'varchar(max)', 'qtycnf': 'varchar(max)', 'qtydlv': 'varchar(max)', 'qtyinv': 'varchar(max)', 'qtyord': 'varchar(max)', 'qtypck': 'varchar(max)', 'qtypkd': 'varchar(max)', 'qtypnd': 'varchar(max)', 'qtyprc': 'varchar(max)', 'qtyprd': 'varchar(max)', 'qtyrcv': 'varchar(max)', 'qtyret': 'varchar(max)', 'qtyrsv': 'varchar(max)', 'qtysld': 'varchar(max)', 'rcvdat': 'varchar(max)', 'rcvfrm': 'varchar(max)', 'rcvunt': 'varchar(max)', 'refcod': 'varchar(max)', 'reftyp': 'varchar(max)', 'regdat': 'varchar(max)', 'requsr': 'varchar(max)', 'seqnum': 'varchar(max)', 'sifcod': 'varchar(max)', 'sixcod': 'varchar(max)', 'sornum': 'varchar(max)', 'splcod': 'varchar(max)', 'stcupd': 'varchar(max)', 'sysddn': 'varchar(max)', 'sysddt': 'varchar(max)', 'sysddv': 'varchar(max)', 'totadc': 'varchar(max)', 'totaic': 'varchar(max)', 'totddp': 'varchar(max)', 'totddv': 'varchar(max)', 'totidp': 'varchar(max)', 'totidv': 'varchar(max)', 'totval': 'varchar(max)', 'trnrra': 'varchar(max)', 'trnrty': 'varchar(max)', 'trnunt': 'varchar(max)', 'txtgen': 'varchar(max)', 'txtinv': 'varchar(max)', 'txtitm': 'varchar(max)', 'vatcod': 'varchar(max)', 'vdmtyp': 'varchar(max)', 'vrscod': 'varchar(max)', 'whscod': 'varchar(max)', 'wkgtyp': 'varchar(max)', 'xtccod': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['compny', 'linnum', 'sornum']
    ),
    start=start,
    cron="0 2 * * *",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_md_rainbow_sol_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_md_rainbow_sol (_data_modified_utc)"]
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
		CAST(actcst AS VARCHAR(MAX)) AS actcst,
		CONVERT(varchar(max), actdat, 126) AS actdat,
		CAST(adsseq AS VARCHAR(MAX)) AS adsseq,
		CAST(agncod AS VARCHAR(MAX)) AS agncod,
		CAST(agrnum AS VARCHAR(MAX)) AS agrnum,
		CAST(am2typ AS VARCHAR(MAX)) AS am2typ,
		CAST(bodseq AS VARCHAR(MAX)) AS bodseq,
		CAST(cbchld AS VARCHAR(MAX)) AS cbchld,
		CAST(cbycod AS VARCHAR(MAX)) AS cbycod,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CONVERT(varchar(max), cnfdsp, 126) AS cnfdsp,
		CONVERT(varchar(max), cnfrcv, 126) AS cnfrcv,
		CAST(cntori AS VARCHAR(MAX)) AS cntori,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creref AS VARCHAR(MAX)) AS creref,
		CAST(creseq AS VARCHAR(MAX)) AS creseq,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(cstcod AS VARCHAR(MAX)) AS cstcod,
		CAST(cstprc AS VARCHAR(MAX)) AS cstprc,
		CAST(ctfseq AS VARCHAR(MAX)) AS ctfseq,
		CAST(cwsbtr AS VARCHAR(MAX)) AS cwsbtr,
		CAST(dfiseq AS VARCHAR(MAX)) AS dfiseq,
		CONVERT(varchar(max), dlvdat, 126) AS dlvdat,
		CAST(dlvmrk AS VARCHAR(MAX)) AS dlvmrk,
		CAST(dptcod AS VARCHAR(MAX)) AS dptcod,
		CONVERT(varchar(max), dspdat, 126) AS dspdat,
		CAST(earcod AS VARCHAR(MAX)) AS earcod,
		CAST(edists AS VARCHAR(MAX)) AS edists,
		CAST(entfrm AS VARCHAR(MAX)) AS entfrm,
		CAST(estcst AS VARCHAR(MAX)) AS estcst,
		CAST(etocod AS VARCHAR(MAX)) AS etocod,
		CAST(excexd AS VARCHAR(MAX)) AS excexd,
		CAST(excidn AS VARCHAR(MAX)) AS excidn,
		CAST(excivd AS VARCHAR(MAX)) AS excivd,
		CAST(exctrd AS VARCHAR(MAX)) AS exctrd,
		CONVERT(varchar(max), expdat, 126) AS expdat,
		CAST(extcod AS VARCHAR(MAX)) AS extcod,
		CAST(extitm AS VARCHAR(MAX)) AS extitm,
		CAST(extnam AS VARCHAR(MAX)) AS extnam,
		CAST(extseq AS VARCHAR(MAX)) AS extseq,
		CAST(grsam2 AS VARCHAR(MAX)) AS grsam2,
		CAST(grsvdm AS VARCHAR(MAX)) AS grsvdm,
		CAST(grswkg AS VARCHAR(MAX)) AS grswkg,
		CAST(icscat AS VARCHAR(MAX)) AS icscat,
		CAST(icsref AS VARCHAR(MAX)) AS icsref,
		CAST(idfent AS VARCHAR(MAX)) AS idfent,
		CAST(idncod AS VARCHAR(MAX)) AS idncod,
		CAST(idnsel AS VARCHAR(MAX)) AS idnsel,
		CAST(invnow AS VARCHAR(MAX)) AS invnow,
		CAST(ipiadd AS VARCHAR(MAX)) AS ipiadd,
		CAST(itmadd AS VARCHAR(MAX)) AS itmadd,
		CAST(kepcst AS VARCHAR(MAX)) AS kepcst,
		CAST(kitseq AS VARCHAR(MAX)) AS kitseq,
		CAST(lincod AS VARCHAR(MAX)) AS lincod,
		CAST(linnam AS VARCHAR(MAX)) AS linnam,
		CAST(linnum AS VARCHAR(MAX)) AS linnum,
		CAST(lintyp AS VARCHAR(MAX)) AS lintyp,
		CONVERT(varchar(max), lstdat, 126) AS lstdat,
		CAST(lststs AS VARCHAR(MAX)) AS lststs,
		CAST(manddn AS VARCHAR(MAX)) AS manddn,
		CAST(manddt AS VARCHAR(MAX)) AS manddt,
		CAST(manddv AS VARCHAR(MAX)) AS manddv,
		CAST(mstads AS VARCHAR(MAX)) AS mstads,
		CAST(mstctf AS VARCHAR(MAX)) AS mstctf,
		CAST(mstkit AS VARCHAR(MAX)) AS mstkit,
		CAST(netam2 AS VARCHAR(MAX)) AS netam2,
		CAST(netvdm AS VARCHAR(MAX)) AS netvdm,
		CAST(netwkg AS VARCHAR(MAX)) AS netwkg,
		CAST(numpal AS VARCHAR(MAX)) AS numpal,
		CAST(numpcl AS VARCHAR(MAX)) AS numpcl,
		CAST(ofmcod AS VARCHAR(MAX)) AS ofmcod,
		CAST(ofmreq AS VARCHAR(MAX)) AS ofmreq,
		CAST(onhold AS VARCHAR(MAX)) AS onhold,
		CAST(orgitm AS VARCHAR(MAX)) AS orgitm,
		CAST(ownadd AS VARCHAR(MAX)) AS ownadd,
		CONVERT(varchar(max), owndat, 126) AS owndat,
		CONVERT(varchar(max), pckdat, 126) AS pckdat,
		CAST(posnum AS VARCHAR(MAX)) AS posnum,
		CAST(prcact AS VARCHAR(MAX)) AS prcact,
		CAST(prcbas AS VARCHAR(MAX)) AS prcbas,
		CAST(prccst AS VARCHAR(MAX)) AS prccst,
		CAST(prcdim AS VARCHAR(MAX)) AS prcdim,
		CAST(prcgrs AS VARCHAR(MAX)) AS prcgrs,
		CAST(prcmth AS VARCHAR(MAX)) AS prcmth,
		CAST(prcrsv AS VARCHAR(MAX)) AS prcrsv,
		CAST(prdcst AS VARCHAR(MAX)) AS prdcst,
		CAST(prdrsv AS VARCHAR(MAX)) AS prdrsv,
		CAST(prjcod AS VARCHAR(MAX)) AS prjcod,
		CAST(prttxg AS VARCHAR(MAX)) AS prttxg,
		CAST(prttxi AS VARCHAR(MAX)) AS prttxi,
		CAST(q2barr AS VARCHAR(MAX)) AS q2barr,
		CAST(q2basm AS VARCHAR(MAX)) AS q2basm,
		CAST(q2bcnf AS VARCHAR(MAX)) AS q2bcnf,
		CAST(q2bdlv AS VARCHAR(MAX)) AS q2bdlv,
		CAST(q2bext AS VARCHAR(MAX)) AS q2bext,
		CAST(q2binv AS VARCHAR(MAX)) AS q2binv,
		CAST(q2bpck AS VARCHAR(MAX)) AS q2bpck,
		CAST(q2bpkd AS VARCHAR(MAX)) AS q2bpkd,
		CAST(q2bprc AS VARCHAR(MAX)) AS q2bprc,
		CAST(q2bprd AS VARCHAR(MAX)) AS q2bprd,
		CAST(q2brcv AS VARCHAR(MAX)) AS q2brcv,
		CAST(q2bret AS VARCHAR(MAX)) AS q2bret,
		CAST(q2brsv AS VARCHAR(MAX)) AS q2brsv,
		CAST(qtyarr AS VARCHAR(MAX)) AS qtyarr,
		CAST(qtyasm AS VARCHAR(MAX)) AS qtyasm,
		CAST(qtyava AS VARCHAR(MAX)) AS qtyava,
		CAST(qtycnf AS VARCHAR(MAX)) AS qtycnf,
		CAST(qtydlv AS VARCHAR(MAX)) AS qtydlv,
		CAST(qtyinv AS VARCHAR(MAX)) AS qtyinv,
		CAST(qtyord AS VARCHAR(MAX)) AS qtyord,
		CAST(qtypck AS VARCHAR(MAX)) AS qtypck,
		CAST(qtypkd AS VARCHAR(MAX)) AS qtypkd,
		CAST(qtypnd AS VARCHAR(MAX)) AS qtypnd,
		CAST(qtyprc AS VARCHAR(MAX)) AS qtyprc,
		CAST(qtyprd AS VARCHAR(MAX)) AS qtyprd,
		CAST(qtyrcv AS VARCHAR(MAX)) AS qtyrcv,
		CAST(qtyret AS VARCHAR(MAX)) AS qtyret,
		CAST(qtyrsv AS VARCHAR(MAX)) AS qtyrsv,
		CAST(qtysld AS VARCHAR(MAX)) AS qtysld,
		CONVERT(varchar(max), rcvdat, 126) AS rcvdat,
		CAST(rcvfrm AS VARCHAR(MAX)) AS rcvfrm,
		CAST(rcvunt AS VARCHAR(MAX)) AS rcvunt,
		CAST(refcod AS VARCHAR(MAX)) AS refcod,
		CAST(reftyp AS VARCHAR(MAX)) AS reftyp,
		CONVERT(varchar(max), regdat, 126) AS regdat,
		CAST(requsr AS VARCHAR(MAX)) AS requsr,
		CAST(seqnum AS VARCHAR(MAX)) AS seqnum,
		CAST(sifcod AS VARCHAR(MAX)) AS sifcod,
		CAST(sixcod AS VARCHAR(MAX)) AS sixcod,
		CAST(sornum AS VARCHAR(MAX)) AS sornum,
		CAST(splcod AS VARCHAR(MAX)) AS splcod,
		CAST(stcupd AS VARCHAR(MAX)) AS stcupd,
		CAST(sysddn AS VARCHAR(MAX)) AS sysddn,
		CAST(sysddt AS VARCHAR(MAX)) AS sysddt,
		CAST(sysddv AS VARCHAR(MAX)) AS sysddv,
		CAST(totadc AS VARCHAR(MAX)) AS totadc,
		CAST(totaic AS VARCHAR(MAX)) AS totaic,
		CAST(totddp AS VARCHAR(MAX)) AS totddp,
		CAST(totddv AS VARCHAR(MAX)) AS totddv,
		CAST(totidp AS VARCHAR(MAX)) AS totidp,
		CAST(totidv AS VARCHAR(MAX)) AS totidv,
		CAST(totval AS VARCHAR(MAX)) AS totval,
		CAST(trnrra AS VARCHAR(MAX)) AS trnrra,
		CAST(trnrty AS VARCHAR(MAX)) AS trnrty,
		CAST(trnunt AS VARCHAR(MAX)) AS trnunt,
		CAST(txtgen AS VARCHAR(MAX)) AS txtgen,
		CAST(txtinv AS VARCHAR(MAX)) AS txtinv,
		CAST(txtitm AS VARCHAR(MAX)) AS txtitm,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vdmtyp AS VARCHAR(MAX)) AS vdmtyp,
		CAST(vrscod AS VARCHAR(MAX)) AS vrscod,
		CAST(whscod AS VARCHAR(MAX)) AS whscod,
		CAST(wkgtyp AS VARCHAR(MAX)) AS wkgtyp,
		CAST(xtccod AS VARCHAR(MAX)) AS xtccod 
	FROM Rainbow_MD.rainbow.sol
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    