#@(#) Nom              : trans_tiers.sql
#@(#) Date de creation : 30/03/2018
#@(#) -----------------------------------------------------------------------------------
#@(#) Description      : Alimentation table TIERS_TMP



set hive.exec.parallel=true;

ALTER TABLE ${BDD_LEASING_DATA_TMP}.tiers_tmp RENAME TO ${BDD_LEASING_DATA_TMP}.agr_tiers_tmp1;

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.tiers_tmp;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.tiers_tmp AS
SELECT
  t1.*, -- commentaire
  t2.partenaire_segment_code,
  t2.partenaire_segment, -- autre commentaire
  t3.partenaire_marche_code,
  t3.partenaire_marche,
  t4.libelle_niveau1,
  t4.c_code_crca_maitre,
  "test de commentaire -- en chaine" as test
FROM ${BDD_LEASING_DATA_TMP}.agr_tiers_tmp1 t1
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_bt_roles t2
ON (t1.ie_tiers=t2.ie_tiers)
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_bt_agrement t3
ON (t1.ie_tiers=t3.ie_tiers)
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_bt_corres_crca t4
ON (t1.ie_tiers=t4.ie_tiers);

DROP TABLE ${BDD_LEASING_DATA_TMP}.agr_tiers_tmp1;
