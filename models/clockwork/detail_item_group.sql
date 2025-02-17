MODEL (
  kind FULL
);

SELECT 'DS' as source,
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
SELECT 'KS' as source,
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
SELECT 'MD' as source,
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
SELECT 'SLSO' as source,
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
SELECT 'SOS' as source,
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
SELECT 'ST' as source,
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
SELECT 'TH' as source,
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