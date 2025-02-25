MODEL (
  kind FULL
);

SELECT CAST('DS' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_DS_rainbow_dig
UNION ALL
SELECT cast('KS' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_KS_rainbow_dig
UNION ALL
SELECT cast('MD' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_MD_rainbow_dig
UNION ALL
SELECT cast('SLSO' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_SLSO_rainbow_dig
UNION ALL
SELECT cast('SOS' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_SOS_rainbow_dig
UNION ALL
SELECT cast('ST' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_ST_rainbow_dig
UNION ALL
SELECT cast('TH' AS VARCHAR(50)) as source,
chgdat as 'Information_Last_Changed_Date',
chgusr as 'Information_Last_Changed_by_User',
compny as 'Company_Identification_Code',
credat as 'Information_Created_Date',
creusr as 'Information_Created_by_User',
digcod as 'Detail_Item_Group_Identification_Code',
dignam as 'Name___Description',
migcod as 'Main_Item_Group_Identification_Code',
sigcod as 'Sub_Item_Group_Identification_Code',
srtnam as 'Short_Name',
srtnum as 'Short_Number',
txtdsc as 'Additional_Text___Description'
FROM clockwork.sllclockdb01_dc_sll_se_Rainbow_TH_rainbow_dig