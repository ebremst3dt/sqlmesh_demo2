IF DB_ID('CLEAN') IS NULL
CREATE DATABASE CLEAN;
GO

IF DB_ID('CONSUME') IS NULL
CREATE DATABASE CONSUME;
GO

IF DB_ID('CLOCKWORK_SLLCLOCKDB01_dc_sll_se') IS NULL
CREATE DATABASE CLOCKWORK_SLLCLOCKDB01_dc_sll_se;
GO

USE CLOCKWORK_SLLCLOCKDB01_dc_sll_se;

IF NOT EXISTS (SELECT TOP 1000 * FROM sys.schemas WHERE name = 'RAINBOW_KS')
BEGIN
    EXEC('CREATE SCHEMA RAINBOW_KS');
END
GO

IF DB_ID('raindance_lsfp3_rd_sll_se') IS NULL
CREATE DATABASE raindance_lsfp3_rd_sll_se;
GO

USE raindance_lsfp3_rd_sll_se;

-- IF NOT EXISTS (SELECT TOP 1000 * FROM sys.schemas WHERE name = 'rainbow')
-- BEGIN
--     EXEC('CREATE SCHEMA rainbow');
-- END;
-- GO

IF NOT EXISTS (SELECT TOP 1000 * FROM sys.schemas WHERE name = 'utdata_utdata295')
BEGIN
    EXEC('CREATE SCHEMA utdata_utdata295');
END
GO