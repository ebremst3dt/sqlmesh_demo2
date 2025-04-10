
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from ingest.mssql import read
from data_load_parameters.clockwork import start

    
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source_catalog': 'varchar(max)', 'agrnum': 'varchar(max)', 'catbuy': 'varchar(max)', 'cbccnl': 'varchar(max)', 'cbcent': 'varchar(max)', 'cbchld': 'varchar(max)', 'chgdat': 'varchar(max)', 'chgrtd': 'varchar(max)', 'chgrtt': 'varchar(max)', 'chgrtv': 'varchar(max)', 'chgusr': 'varchar(max)', 'cnfdsp': 'varchar(max)', 'cnfnum': 'varchar(max)', 'cnfrcv': 'varchar(max)', 'cntori': 'varchar(max)', 'compny': 'varchar(max)', 'credat': 'varchar(max)', 'creusr': 'varchar(max)', 'csccod': 'varchar(max)', 'cstcod': 'varchar(max)', 'cstprc': 'varchar(max)', 'ctfseq': 'varchar(max)', 'dfiseq': 'varchar(max)', 'dlvdat': 'varchar(max)', 'dlvmrk': 'varchar(max)', 'dptcod': 'varchar(max)', 'dsctyp': 'varchar(max)', 'dspdat': 'varchar(max)', 'dstper': 'varchar(max)', 'earcod': 'varchar(max)', 'extitm': 'varchar(max)', 'extnam': 'varchar(max)', 'extreq': 'varchar(max)', 'extseq': 'varchar(max)', 'grsvdm': 'varchar(max)', 'grswkg': 'varchar(max)', 'icscat': 'varchar(max)', 'icsref': 'varchar(max)', 'idncod': 'varchar(max)', 'itkseq': 'varchar(max)', 'lincod': 'varchar(max)', 'linnam': 'varchar(max)', 'linnum': 'varchar(max)', 'lintyp': 'varchar(max)', 'lstdat': 'varchar(max)', 'lststs': 'varchar(max)', 'manddn': 'varchar(max)', 'manddt': 'varchar(max)', 'manddv': 'varchar(max)', 'matcst': 'varchar(max)', 'mstctf': 'varchar(max)', 'mstidn': 'varchar(max)', 'mstmat': 'varchar(max)', 'mstnot': 'varchar(max)', 'mstqac': 'varchar(max)', 'mstqar': 'varchar(max)', 'mstrmp': 'varchar(max)', 'netvdm': 'varchar(max)', 'netwkg': 'varchar(max)', 'numpal': 'varchar(max)', 'numpcl': 'varchar(max)', 'ofmcod': 'varchar(max)', 'ofmreq': 'varchar(max)', 'onhold': 'varchar(max)', 'orqdat': 'varchar(max)', 'ownseq': 'varchar(max)', 'plnsts': 'varchar(max)', 'pornum': 'varchar(max)', 'posnum': 'varchar(max)', 'prcact': 'varchar(max)', 'prcbas': 'varchar(max)', 'prcinv': 'varchar(max)', 'prjcod': 'varchar(max)', 'q2barr': 'varchar(max)', 'q2bcnf': 'varchar(max)', 'q2bdsc': 'varchar(max)', 'q2binv': 'varchar(max)', 'q2bqac': 'varchar(max)', 'q2brcv': 'varchar(max)', 'qtyarr': 'varchar(max)', 'qtycnf': 'varchar(max)', 'qtydsc': 'varchar(max)', 'qtyinv': 'varchar(max)', 'qtyord': 'varchar(max)', 'qtyprc': 'varchar(max)', 'qtyqac': 'varchar(max)', 'qtyrcv': 'varchar(max)', 'qtysad': 'varchar(max)', 'rcvdat': 'varchar(max)', 'regdat': 'varchar(max)', 'reqnum': 'varchar(max)', 'reqseq': 'varchar(max)', 'reqtrn': 'varchar(max)', 'reqtyp': 'varchar(max)', 'requsr': 'varchar(max)', 'revnum': 'varchar(max)', 'rplref': 'varchar(max)', 'seqnum': 'varchar(max)', 'stcupd': 'varchar(max)', 'sysddn': 'varchar(max)', 'sysddt': 'varchar(max)', 'sysddv': 'varchar(max)', 'tatadc': 'varchar(max)', 'totaic': 'varchar(max)', 'totddp': 'varchar(max)', 'totddv': 'varchar(max)', 'totidp': 'varchar(max)', 'totidv': 'varchar(max)', 'totval': 'varchar(max)', 'trnrra': 'varchar(max)', 'trnrty': 'varchar(max)', 'trnunt': 'varchar(max)', 'txtgen': 'varchar(max)', 'txtitm': 'varchar(max)', 'vatcod': 'varchar(max)', 'vdmtyp': 'varchar(max)', 'vrscod': 'varchar(max)', 'whscod': 'varchar(max)', 'wkgtyp': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        batch_size=30,
        unique_key=['compny', 'linnum', 'pornum']
    ),
    start=start,
    cron="0 2 * * *",
    post_statements=["CREATE INDEX IF NOT EXISTS sllclockdb01_dc_sll_se_rainbow_ks_rainbow_pol_data_modified_utc ON clockwork_sllclockdb01_dc_sll_se.rainbow_ks_rainbow_pol (_data_modified_utc)"]
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
		'Rainbow_KS' as _source_catalog,
		CAST(agrnum AS VARCHAR(MAX)) AS agrnum,
		CAST(catbuy AS VARCHAR(MAX)) AS catbuy,
		CAST(cbccnl AS VARCHAR(MAX)) AS cbccnl,
		CAST(cbcent AS VARCHAR(MAX)) AS cbcent,
		CAST(cbchld AS VARCHAR(MAX)) AS cbchld,
		CONVERT(varchar(max), chgdat, 126) AS chgdat,
		CAST(chgrtd AS VARCHAR(MAX)) AS chgrtd,
		CAST(chgrtt AS VARCHAR(MAX)) AS chgrtt,
		CAST(chgrtv AS VARCHAR(MAX)) AS chgrtv,
		CAST(chgusr AS VARCHAR(MAX)) AS chgusr,
		CONVERT(varchar(max), cnfdsp, 126) AS cnfdsp,
		CAST(cnfnum AS VARCHAR(MAX)) AS cnfnum,
		CONVERT(varchar(max), cnfrcv, 126) AS cnfrcv,
		CAST(cntori AS VARCHAR(MAX)) AS cntori,
		CAST(compny AS VARCHAR(MAX)) AS compny,
		CONVERT(varchar(max), credat, 126) AS credat,
		CAST(creusr AS VARCHAR(MAX)) AS creusr,
		CAST(csccod AS VARCHAR(MAX)) AS csccod,
		CAST(cstcod AS VARCHAR(MAX)) AS cstcod,
		CAST(cstprc AS VARCHAR(MAX)) AS cstprc,
		CAST(ctfseq AS VARCHAR(MAX)) AS ctfseq,
		CAST(dfiseq AS VARCHAR(MAX)) AS dfiseq,
		CONVERT(varchar(max), dlvdat, 126) AS dlvdat,
		CAST(dlvmrk AS VARCHAR(MAX)) AS dlvmrk,
		CAST(dptcod AS VARCHAR(MAX)) AS dptcod,
		CAST(dsctyp AS VARCHAR(MAX)) AS dsctyp,
		CONVERT(varchar(max), dspdat, 126) AS dspdat,
		CAST(dstper AS VARCHAR(MAX)) AS dstper,
		CAST(earcod AS VARCHAR(MAX)) AS earcod,
		CAST(extitm AS VARCHAR(MAX)) AS extitm,
		CAST(extnam AS VARCHAR(MAX)) AS extnam,
		CAST(extreq AS VARCHAR(MAX)) AS extreq,
		CAST(extseq AS VARCHAR(MAX)) AS extseq,
		CAST(grsvdm AS VARCHAR(MAX)) AS grsvdm,
		CAST(grswkg AS VARCHAR(MAX)) AS grswkg,
		CAST(icscat AS VARCHAR(MAX)) AS icscat,
		CAST(icsref AS VARCHAR(MAX)) AS icsref,
		CAST(idncod AS VARCHAR(MAX)) AS idncod,
		CAST(itkseq AS VARCHAR(MAX)) AS itkseq,
		CAST(lincod AS VARCHAR(MAX)) AS lincod,
		CAST(linnam AS VARCHAR(MAX)) AS linnam,
		CAST(linnum AS VARCHAR(MAX)) AS linnum,
		CAST(lintyp AS VARCHAR(MAX)) AS lintyp,
		CONVERT(varchar(max), lstdat, 126) AS lstdat,
		CAST(lststs AS VARCHAR(MAX)) AS lststs,
		CAST(manddn AS VARCHAR(MAX)) AS manddn,
		CAST(manddt AS VARCHAR(MAX)) AS manddt,
		CAST(manddv AS VARCHAR(MAX)) AS manddv,
		CAST(matcst AS VARCHAR(MAX)) AS matcst,
		CAST(mstctf AS VARCHAR(MAX)) AS mstctf,
		CAST(mstidn AS VARCHAR(MAX)) AS mstidn,
		CAST(mstmat AS VARCHAR(MAX)) AS mstmat,
		CAST(mstnot AS VARCHAR(MAX)) AS mstnot,
		CAST(mstqac AS VARCHAR(MAX)) AS mstqac,
		CAST(mstqar AS VARCHAR(MAX)) AS mstqar,
		CAST(mstrmp AS VARCHAR(MAX)) AS mstrmp,
		CAST(netvdm AS VARCHAR(MAX)) AS netvdm,
		CAST(netwkg AS VARCHAR(MAX)) AS netwkg,
		CAST(numpal AS VARCHAR(MAX)) AS numpal,
		CAST(numpcl AS VARCHAR(MAX)) AS numpcl,
		CAST(ofmcod AS VARCHAR(MAX)) AS ofmcod,
		CAST(ofmreq AS VARCHAR(MAX)) AS ofmreq,
		CAST(onhold AS VARCHAR(MAX)) AS onhold,
		CONVERT(varchar(max), orqdat, 126) AS orqdat,
		CAST(ownseq AS VARCHAR(MAX)) AS ownseq,
		CAST(plnsts AS VARCHAR(MAX)) AS plnsts,
		CAST(pornum AS VARCHAR(MAX)) AS pornum,
		CAST(posnum AS VARCHAR(MAX)) AS posnum,
		CAST(prcact AS VARCHAR(MAX)) AS prcact,
		CAST(prcbas AS VARCHAR(MAX)) AS prcbas,
		CAST(prcinv AS VARCHAR(MAX)) AS prcinv,
		CAST(prjcod AS VARCHAR(MAX)) AS prjcod,
		CAST(q2barr AS VARCHAR(MAX)) AS q2barr,
		CAST(q2bcnf AS VARCHAR(MAX)) AS q2bcnf,
		CAST(q2bdsc AS VARCHAR(MAX)) AS q2bdsc,
		CAST(q2binv AS VARCHAR(MAX)) AS q2binv,
		CAST(q2bqac AS VARCHAR(MAX)) AS q2bqac,
		CAST(q2brcv AS VARCHAR(MAX)) AS q2brcv,
		CAST(qtyarr AS VARCHAR(MAX)) AS qtyarr,
		CAST(qtycnf AS VARCHAR(MAX)) AS qtycnf,
		CAST(qtydsc AS VARCHAR(MAX)) AS qtydsc,
		CAST(qtyinv AS VARCHAR(MAX)) AS qtyinv,
		CAST(qtyord AS VARCHAR(MAX)) AS qtyord,
		CAST(qtyprc AS VARCHAR(MAX)) AS qtyprc,
		CAST(qtyqac AS VARCHAR(MAX)) AS qtyqac,
		CAST(qtyrcv AS VARCHAR(MAX)) AS qtyrcv,
		CAST(qtysad AS VARCHAR(MAX)) AS qtysad,
		CONVERT(varchar(max), rcvdat, 126) AS rcvdat,
		CONVERT(varchar(max), regdat, 126) AS regdat,
		CAST(reqnum AS VARCHAR(MAX)) AS reqnum,
		CAST(reqseq AS VARCHAR(MAX)) AS reqseq,
		CAST(reqtrn AS VARCHAR(MAX)) AS reqtrn,
		CAST(reqtyp AS VARCHAR(MAX)) AS reqtyp,
		CAST(requsr AS VARCHAR(MAX)) AS requsr,
		CAST(revnum AS VARCHAR(MAX)) AS revnum,
		CAST(rplref AS VARCHAR(MAX)) AS rplref,
		CAST(seqnum AS VARCHAR(MAX)) AS seqnum,
		CAST(stcupd AS VARCHAR(MAX)) AS stcupd,
		CAST(sysddn AS VARCHAR(MAX)) AS sysddn,
		CAST(sysddt AS VARCHAR(MAX)) AS sysddt,
		CAST(sysddv AS VARCHAR(MAX)) AS sysddv,
		CAST(tatadc AS VARCHAR(MAX)) AS tatadc,
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
		CAST(txtitm AS VARCHAR(MAX)) AS txtitm,
		CAST(vatcod AS VARCHAR(MAX)) AS vatcod,
		CAST(vdmtyp AS VARCHAR(MAX)) AS vdmtyp,
		CAST(vrscod AS VARCHAR(MAX)) AS vrscod,
		CAST(whscod AS VARCHAR(MAX)) AS whscod,
		CAST(wkgtyp AS VARCHAR(MAX)) AS wkgtyp 
	FROM Rainbow_KS.rainbow.pol
     )y
    WHERE _data_modified_utc between '{start}' and '{end}'
    
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
    