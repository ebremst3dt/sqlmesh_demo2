IF DB_ID('Rainbow_KS') IS NULL
GO

CREATE DATABASE Rainbow_KS;
GO

USE Rainbow_KS;

IF NOT EXISTS (SELECT TOP 1000 * FROM sys.schemas WHERE name = 'rainbow')
BEGIN
    EXEC('GO

CREATE SCHEMA rainbow');
END;
GO

-- Create table
GO

CREATE TABLE rainbow.dig (
    chgdat datetime,
    chgusr char(10),
    compny char(2),
    credat datetime,
    creusr char(10),
    digcod char(10),
    dignam varchar(40),
    migcod char(10),
    sigcod char(10),
    srtnam char(10),
    srtnum char(10),
    txtdsc varchar(max)  -- SQL Server uses varchar(max) instead of varchar(-1)
);

-- Insert sample rows
GO

INSERT INTO rainbow.dig (
    chgdat, chgusr, compny, credat, creusr, digcod,
    dignam, migcod, sigcod, srtnam, srtnum, txtdsc
) VALUES (
    '2024-01-22 10:30:00', 'JOHNDOE', 'US', '2024-01-22 09:00:00', 'JOHNDOE', 'DIG001',
    'First Digest Entry', 'MIG100', 'SIG500', 'SORT1', 'NUM001', 'This is the first digest description'
),
(
    '2024-01-22 14:45:00', 'JANESMITH', 'UK', '2024-01-22 14:00:00', 'JANESMITH', 'DIG002',
    'Second Digest Entry', 'MIG200', 'SIG501', 'SORT2', 'NUM002', 'This is the second digest description'
);






IF DB_ID('Rainbow_DS') IS NULL
GO

CREATE DATABASE Rainbow_DS;
GO

USE Rainbow_DS;

IF NOT EXISTS (SELECT TOP 1000 * FROM sys.schemas WHERE name = 'rainbow')
BEGIN
    EXEC('GO

CREATE SCHEMA rainbow');
END;
GO

-- Create table
GO

CREATE TABLE rainbow.dig (
    chgdat datetime,
    chgusr char(10),
    compny char(2),
    credat datetime,
    creusr char(10),
    digcod char(10),
    dignam varchar(40),
    migcod char(10),
    sigcod char(10),
    srtnam char(10),
    srtnum char(10),
    txtdsc varchar(max)  -- SQL Server uses varchar(max) instead of varchar(-1)
);

-- Insert sample rows
GO

INSERT INTO rainbow.dig (
    chgdat, chgusr, compny, credat, creusr, digcod,
    dignam, migcod, sigcod, srtnam, srtnum, txtdsc
) VALUES (
    '2024-01-22 10:30:00', 'JOHNDOE', 'US', '2024-01-22 09:00:00', 'JOHNDOE', 'DIG001',
    'First Digest Entry', 'MIG100', 'SIG500', 'SORT1', 'NUM001', 'This is the first digest description'
),
(
    '2024-01-22 14:45:00', 'JANESMITH', 'UK', '2024-01-22 14:00:00', 'JANESMITH', 'DIG002',
    'Second Digest Entry', 'MIG200', 'SIG501', 'SORT2', 'NUM002', 'This is the second digest description'
);



IF DB_ID('Utdata') IS NULL
GO

CREATE DATABASE Utdata;
GO

USE Utdata;

IF NOT EXISTS (SELECT TOP 1000 * FROM sys.schemas WHERE name = 'utdata295')
BEGIN
    EXEC('GO

CREATE SCHEMA utdata295');
END;
GO


GO

CREATE TABLE utdata295.EK_FAKTA_VERIFIKAT (
[VERNR] numeric(10,0) NOT NULL
    , [VERRAD] numeric(5,0) NOT NULL
    , [VERDATUM] datetime NOT NULL
    , [VERTYP] varchar(6) NULL
    , [IB] varchar(1) NULL
    , [STATUS] varchar(1) NULL
    , [DEFDATUM] datetime NULL
    , [DEFSIGN] varchar(3) NULL
    , [KATEGORI] numeric(1,0) NULL
    , [REGSIGN] varchar(3) NULL
    , [REGDATUM] datetime NULL
    , [ATTESTSIGN1] varchar(3) NULL
    , [ATTESTDATUM1] datetime NULL
    , [ATTESTSIGN2] varchar(3) NULL
    , [ATTESTDATUM2] datetime NULL
    , [KONTSIGN] varchar(3) NULL
    , [RADTYPNR] numeric(5,0) NULL
    , [INTERNVERNR] numeric(10,0) NULL
    , [PNYCKEL] varchar(10) NULL
    , [MED] numeric(5,0) NULL
    , [DOK_ANTAL] numeric(5,0) NULL
    , [DOKTYP] numeric(5,0) NULL
    , [DOKUMENTID] varchar(20) NULL
    , [FORETAG] numeric(10,0) NULL
    , [UTILITY] numeric(5,0) NULL
    , [MOTP_ID] varchar(4) NULL
    , [KONTO_ID] varchar(4) NULL
    , [KST_ID] varchar(5) NULL
    , [VERK_ID] varchar(2) NULL
    , [VALUTA_ID] varchar(3) NULL
    , [AVTBES_ID] varchar(7) NULL
    , [FRI1_ID] varchar(4) NULL
    , [FRI2_ID] varchar(3) NULL
    , [PROJ_ID] varchar(5) NULL
    , [PROC_ID] varchar(1) NULL
    , [ID_ID] varchar(16) NULL
    , [URS_ID] varchar(2) NULL
    , [REGDAT_ID] varchar(20) NULL
    , [YKAT_ID] varchar(4) NULL
    , [FÖPROC_ID] varchar(4) NULL
    , [HUVUDTEXT] varchar(30) NULL
    , [RADTEXT] varchar(30) NULL
    , [URSPTEXT] varchar(30) NULL
    , [VERDOKREF] varchar(200) NULL
    , [EXTERNNR] varchar(27) NULL
    , [EXTERNID] varchar(19) NULL
    , [URSPRUNGS_VERIFIKAT] varchar(27) NULL
    , [EXTERNANM] varchar(80) NULL
    , [UTFALL_V] numeric(15,2) NULL
    , [BUDGET_V] numeric(15,2) NULL
    , [ORGVAL_V] numeric(15,2) NULL
    , [PRG_V] numeric(15,2) NULL
    , [LFRAM_V] numeric(15,2) NULL
    , [BPUTF_V] numeric(15,2) NULL
    , [FÖRBEL_V] numeric(15,2) NULL
);

GO


GO

INSERT INTO utdata295.EK_FAKTA_VERIFIKAT
           ([VERNR]
           ,[VERRAD]
           ,[VERDATUM]
           ,[VERTYP]
           ,[IB]
           ,[STATUS]
           ,[DEFDATUM]
           ,[DEFSIGN]
           ,[KATEGORI]
           ,[REGSIGN]
           ,[REGDATUM]
           ,[ATTESTSIGN1]
           ,[ATTESTDATUM1]
           ,[ATTESTSIGN2]
           ,[ATTESTDATUM2]
           ,[KONTSIGN]
           ,[RADTYPNR]
           ,[INTERNVERNR]
           ,[PNYCKEL]
           ,[MED]
           ,[DOK_ANTAL]
           ,[DOKTYP]
           ,[DOKUMENTID]
           ,[FORETAG]
           ,[UTILITY]
           ,[MOTP_ID]
           ,[KONTO_ID]
           ,[KST_ID]
           ,[VERK_ID]
           ,[VALUTA_ID]
           ,[AVTBES_ID]
           ,[FRI1_ID]
           ,[FRI2_ID]
           ,[PROJ_ID]
           ,[PROC_ID]
           ,[ID_ID]
           ,[URS_ID]
           ,[REGDAT_ID]
           ,[YKAT_ID]
           ,[FÖPROC_ID]
           ,[HUVUDTEXT]
           ,[RADTEXT]
           ,[URSPTEXT]
           ,[VERDOKREF]
           ,[EXTERNNR]
           ,[EXTERNID]
           ,[URSPRUNGS_VERIFIKAT]
           ,[EXTERNANM]
           ,[UTFALL_V]
           ,[BUDGET_V]
           ,[ORGVAL_V]
           ,[PRG_V]
           ,[LFRAM_V]
           ,[BPUTF_V]
           ,[FÖRBEL_V])
     VALUES
           (500000
           ,8
           ,'2005-12-27 00:00:00.000'
           ,'LAEFH'
           ,'N'
           ,'D'
           ,'2006-01-03 00:00:00.000'
           ,''
           ,0
           ,'CHB'
           ,'2006-01-03 00:00:00.000'
           ,''
           ,'1799-12-31 00:00:00.000'
           ,''
           ,'1799-12-31 00:00:00.000'
           ,''
           ,1
           ,787282
           ,''
           ,0
           ,0
           ,0
           ,''
           ,295
           ,1
           ,'9989'
           ,'2440'
           ,'99999'
           ,'99'
           ,'SEK'
           ,''
           ,''
           ,''
           ,''
           ,''
           ,'10000'
           ,'2'
           ,''
           ,''
           ,''
           ,'TELE2/COMVIQ    Prelb  9000000'
           ,''
           ,'TELE2/COMVIQ    Prelb  9000000'
           ,''
           ,'9000000'
           ,'LR    10000'
           ,''
           ,''
           ,1.00
           ,0.00
           ,1.00
           ,0.00
           ,0.00
           ,0.00
           ,0.00
		   )
GO


GO

CREATE SCHEMA udpb4_100;
GO

CREATE SCHEMA rd_1210;
GO

CREATE SCHEMA utdata150;
GO

CREATE SCHEMA utdata156;
GO

CREATE SCHEMA utdata261;
GO

CREATE SCHEMA utdata287;
GO

CREATE SCHEMA utdata289;
GO

CREATE SCHEMA utdata290;
GO

CREATE SCHEMA utdata293;
GO

CREATE SCHEMA utdata294;
GO

CREATE SCHEMA utdata295;
GO

CREATE SCHEMA utdata298;
GO

CREATE SCHEMA utdata361;
GO

CREATE SCHEMA utdata801;
GO

CREATE SCHEMA utdata802;
GO

CREATE SCHEMA utdata805;
GO

CREATE SCHEMA utdata840;
GO

CREATE SCHEMA udp_150;
GO

CREATE SCHEMA udp_600;
GO

CREATE SCHEMA udp_220;
GO

CREATE SCHEMA udp_858;
GO

CREATE SCHEMA ftv_400;


GO

CREATE TABLE [udpb4_100].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [rd_1210].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata150].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata156].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata261].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata287].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata289].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata290].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata293].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata294].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata295].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata298].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata361].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata801].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata802].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata805].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [utdata840].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [udp_150].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [udp_600].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [udp_220].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [udp_858].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);

GO

CREATE TABLE [ftv_400].ek_dim_obj_verk (
    VERK_ID varchar(2),
    VERK_TEXT varchar(100),
    VERK_ID_TEXT varchar(100),
    VERK_GILTIG_FOM datetime,
    VERK_GILTIG_TOM datetime,
    VERK_PASSIV bit,
    VGREN_ID varchar(4),
    VGREN_TEXT varchar(100),
    VGREN_ID_TEXT varchar(100),
    VGREN_GILTIG_FOM datetime,
    VGREN_GILTIG_TOM datetime,
    VGREN_PASSIV bit
);


GO

INSERT INTO [udpb4_100].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [rd_1210].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata150].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata156].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata261].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata287].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata289].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata290].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata293].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata294].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata295].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata298].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata361].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata801].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata802].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata805].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata840].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_150].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_600].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_220].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_858].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [ftv_400].ek_dim_obj_verk VALUES
(' ', ' ', '    ', '1799-12-31', '2999-12-31', 0, ' ', ' ', '      ', '1799-12-31', '2999-12-31', 0),
('11', 'Övergrip politisk ek_dim_obj_verk', '11 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1100', 'Övergrip politisk ek_dim_obj_verk', '1100 Övergrip politisk ek_dim_obj_verk', '1799-12-31', '2999-12-31', 0),
('19', 'Övrig beställarek_dim_obj_verk', '19 Övrig beställarek_dim_obj_verk', '1799-12-31', '2999-12-31', 0, '1902', 'Klimat- o hållbarhet', '1902 Klimat- o hållbarhet', '1799-12-31', '2999-12-31', 0),
('99', 'Administration', '99 Administration', '1799-12-31', '2999-12-31', 0, '9910', 'Administration', '9910 Administration', '1799-12-31', '2999-12-31', 0);






GO

CREATE TABLE [udpb4_100].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [rd_1210].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata150].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata156].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata261].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata287].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata289].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata290].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata293].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata294].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata295].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata298].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata361].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata801].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata802].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata805].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [utdata840].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [udp_150].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [udp_600].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [udp_220].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [udp_858].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

CREATE TABLE [ftv_400].ek_dim_obj_kst (
    KST_ID varchar(5),
    KST_TEXT varchar(100),
    KST_ID_TEXT varchar(100),
    KST_GILTIG_FOM datetime,
    KST_GILTIG_TOM datetime,
    KST_PASSIV bit
);

GO

INSERT INTO [udpb4_100].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [rd_1210].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata150].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata156].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata261].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata287].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata289].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata290].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata293].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata294].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata295].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata298].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata361].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata801].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata802].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata805].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [utdata840].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_150].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_600].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_220].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [udp_858].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);

GO

INSERT INTO [ftv_400].ek_dim_obj_kst VALUES
(' ', ' ', '       ', '1799-12-31', '2999-12-31', 0),
('80000', 'Politik', '80000 Politik', '1799-12-31', '2999-12-31', 0),
('80100', 'Förvaltningsstöd', '80100 Förvaltningsstöd', '1799-12-31', '2999-12-31', 0),
('80200', 'Bidragshantering', '80200 Bidragshantering', '1799-12-31', '2999-12-31', 0),
('80300', 'Verksamhet', '80300 Verksamhet', '1799-12-31', '2999-12-31', 0),
('99999', 'Balans', '99999 Balans', '1799-12-31', '2999-12-31', 0);