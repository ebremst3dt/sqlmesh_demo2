
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'agrnum': 'varchar(20)',
 'catbuy': 'char(1)',
 'cbccnl': 'char(4)',
 'cbcent': 'char(4)',
 'cbchld': 'char(4)',
 'chgdat': 'datetime',
 'chgrtd': 'char(1)',
 'chgrtt': 'char(1)',
 'chgrtv': 'int',
 'chgusr': 'char(6)',
 'cnfdsp': 'datetime',
 'cnfnum': 'int',
 'cnfrcv': 'datetime',
 'cntori': 'char(3)',
 'compny': 'char(2)',
 'credat': 'datetime',
 'creusr': 'char(10)',
 'csccod': 'char(10)',
 'cstcod': 'varchar(20)',
 'cstprc': 'decimal',
 'ctfseq': 'char(14)',
 'dfiseq': 'char(14)',
 'dlvdat': 'datetime',
 'dlvmrk': 'varchar(60)',
 'dptcod': 'char(10)',
 'dsctyp': 'char(1)',
 'dspdat': 'datetime',
 'dstper': 'smallint',
 'earcod': 'char(10)',
 'extitm': 'varchar(30)',
 'extnam': 'varchar(60)',
 'extreq': 'char(14)',
 'extseq': 'char(14)',
 'grsvdm': 'decimal',
 'grswkg': 'decimal',
 'icscat': 'varchar(10)',
 'icsref': 'varchar(20)',
 'idncod': 'varchar(50)',
 'itkseq': 'char(14)',
 'lincod': 'char(20)',
 'linnam': 'char(60)',
 'linnum': 'decimal',
 'lintyp': 'char(1)',
 'lstdat': 'datetime',
 'lststs': 'char(2)',
 'manddn': 'decimal',
 'manddt': 'char(1)',
 'manddv': 'decimal',
 'matcst': 'decimal',
 'mstctf': 'char(1)',
 'mstidn': 'char(1)',
 'mstmat': 'char(1)',
 'mstnot': 'char(1)',
 'mstqac': 'char(1)',
 'mstqar': 'char(1)',
 'mstrmp': 'char(1)',
 'netvdm': 'decimal',
 'netwkg': 'decimal',
 'numpal': 'smallint',
 'numpcl': 'smallint',
 'ofmcod': 'char(10)',
 'ofmreq': 'char(1)',
 'onhold': 'char(1)',
 'orqdat': 'datetime',
 'ownseq': 'char(14)',
 'plnsts': 'char(1)',
 'pornum': 'char(10)',
 'posnum': 'varchar(10)',
 'prcact': 'decimal',
 'prcbas': 'decimal',
 'prcinv': 'decimal',
 'prjcod': 'char(10)',
 'q2barr': 'decimal',
 'q2bcnf': 'decimal',
 'q2bdsc': 'decimal',
 'q2binv': 'decimal',
 'q2bqac': 'decimal',
 'q2brcv': 'decimal',
 'qtyarr': 'decimal',
 'qtycnf': 'decimal',
 'qtydsc': 'decimal',
 'qtyinv': 'decimal',
 'qtyord': 'decimal',
 'qtyprc': 'decimal',
 'qtyqac': 'decimal',
 'qtyrcv': 'decimal',
 'qtysad': 'decimal',
 'rcvdat': 'datetime',
 'regdat': 'datetime',
 'reqnum': 'char(10)',
 'reqseq': 'char(14)',
 'reqtrn': 'char(3)',
 'reqtyp': 'char(3)',
 'requsr': 'char(10)',
 'revnum': 'int',
 'rplref': 'varchar(128)',
 'seqnum': 'char(14)',
 'stcupd': 'char(1)',
 'sysddn': 'decimal',
 'sysddt': 'char(1)',
 'sysddv': 'decimal',
 'tatadc': 'decimal',
 'totaic': 'decimal',
 'totddp': 'decimal',
 'totddv': 'decimal',
 'totidp': 'decimal',
 'totidv': 'decimal',
 'totval': 'decimal',
 'trnrra': 'decimal',
 'trnrty': 'char(1)',
 'trnunt': 'char(6)',
 'txtgen': 'varchar(max)',
 'txtitm': 'varchar(max)',
 'vatcod': 'char(4)',
 'vdmtyp': 'char(1)',
 'vrscod': 'varchar(50)',
 'whscod': 'char(10)',
 'wkgtyp': 'char(1)'},
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
	"""
    return read(query=query, server_url="sllclockdb01.dc.sll.se")
