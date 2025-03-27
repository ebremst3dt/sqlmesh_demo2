
MODEL (
    kind VIEW
);

@UNION(
    'all',
    clockwork_sllclockdb01_dc_sll_se.rainbow_ds_rainbow_mig,
	clockwork_sllclockdb01_dc_sll_se.rainbow_ks_rainbow_mig,
	clockwork_sllclockdb01_dc_sll_se.rainbow_md_rainbow_mig,
	clockwork_sllclockdb01_dc_sll_se.rainbow_slso_rainbow_mig,
	clockwork_sllclockdb01_dc_sll_se.rainbow_sos_rainbow_mig,
	clockwork_sllclockdb01_dc_sll_se.rainbow_st_rainbow_mig,
	clockwork_sllclockdb01_dc_sll_se.rainbow_th_rainbow_mig
)
        