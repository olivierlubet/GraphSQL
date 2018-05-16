
DROP TABLE ${BDD_TMP}.agr_tiers_tmp1;

#@(#) -----------------------------------------------------------------------------------
#@(#) Nom              : trans_projet.sql
#@(#) -----------------------------------------------------------------------------------


set hive.exec.parallel=true;


-- Rapatriement des derniers scores calculés d'un projet DE

DROP TABLE IF EXISTS ${BDD_TMP}.de_projet_score_step1;
CREATE TABLE ${BDD_TMP}.de_projet_score_step1 AS
SELECT n_numero_dossier,
  MAX(n_demande) AS n_demande
FROM ${BDD_COMMUN}.de_reponse_octroi
GROUP BY n_numero_dossier ;

-- Table temporaire permettant la récupération des scores d'un projet DE
DROP TABLE IF EXISTS ${BDD_TMP}.de_projet_score;
CREATE TABLE ${BDD_TMP}.de_projet_score AS
SELECT DISTINCT a.n_numero_dossier AS id_projet_origine,
  CASE
    WHEN b.c_statut = 'ATTENTE'
    THEN 'ORANGE'
    WHEN b.c_statut = 'ACCEPTE'
    THEN 'VERT'
    WHEN b.c_statut = 'REFUSE'
    THEN 'ROUGE'
  END             AS score,
  b.l_commentaire AS score_motif
FROM ${BDD_TMP}.de_projet_score_step1 a
LEFT OUTER JOIN ${BDD_COMMUN}.de_reponse_octroi b
ON (a.n_numero_dossier = b.n_numero_dossier
AND a.n_demande = b.n_demande);

-- Table temporaire permettant la récupération des types de population d'un projet DE
DROP TABLE IF EXISTS ${BDD_TMP}.de_projet_population;
CREATE TABLE ${BDD_TMP}.de_projet_population AS
SELECT a.id_projet_origine,
  b.l_libelle AS modele_population,
  a.code_type_dossier,
  a.code_type_ligne,
  a.code_campagne,
  a.code_produit_financier,
  a.code_detail_prouit,
  a.code_type_produit,
  a.code_lease_back,
  a.code_pool,
  a.d_statut_courant,
  a.ie_utilisateur,
  a.score_libelle,
  a.code_ligne_groupe,
  a.code_score_decision,
  a.code_utilisable,
  a.code_naf,
  a.partenaire_pool_cal,
  a.n_apporteur_principal,
  a.n_apporteur_commissionne,
  a.n_ext_pmp,
  a.n_ext_pmp_agence,
  a.n_ext_pmp_commercial,
  CASE a.c_provenance
    WHEN '11'
    THEN 'Simple'
    WHEN '12'
    THEN 'Simple'
    WHEN '13'
    THEN 'Simple'
    WHEN 'V'
    THEN 'Simple'
    WHEN 'C'
    THEN 'Simple'
    ELSE 'Complexe'
  END AS complexite
FROM ${BDD_TMP}.de_dossier_step1 a
LEFT OUTER JOIN ${BDD_COMMUN}.de_nomenclature b
ON ( a.c_provenance = b.c_nomenclature
AND b.c_type_nomenclature = 'PRDO' );

-- Table temporaire permettant la récupération des modèles de marché d'un projet DE
DROP TABLE IF EXISTS ${BDD_TMP}.de_projet_marche;
CREATE TABLE ${BDD_TMP}.de_projet_marche AS
SELECT a.id_projet_origine,
  b.l_libelle AS modele_marche
FROM ${BDD_TMP}.de_dossier_step1 a
LEFT OUTER JOIN ${BDD_COMMUN}.de_nomenclature b
ON ( a.c_marche_ccf = b.c_nomenclature
AND b.c_type_nomenclature = 'MCCF' );

-- Table temporaire stockant les modèle de marché et de population des dossiers DE
DROP TABLE IF EXISTS ${BDD_TMP}.de_projet_step1;
CREATE TABLE ${BDD_TMP}.de_projet_step1 AS
SELECT
  CASE
    WHEN a.id_projet_origine IS NULL
    THEN b.id_projet_origine
    ELSE a.id_projet_origine
  END AS id_projet_origine,
  b.modele_marche,
  a.modele_population,
  a.complexite
FROM ${BDD_TMP}.de_projet_population a
FULL OUTER JOIN ${BDD_TMP}.de_projet_marche b
ON ( a.id_projet_origine = b.id_projet_origine );

-- Table temporaire stockant les modèle de marché, de population et le score des dossiers DE
DROP TABLE IF EXISTS ${BDD_TMP}.de_projet_step2;
CREATE TABLE ${BDD_TMP}.de_projet_step2 AS
SELECT
  CASE
    WHEN a.id_projet_origine IS NULL
    THEN b.id_projet_origine
    ELSE a.id_projet_origine
  END AS id_projet_origine,
  a.modele_marche,
  a.modele_population,
  a.complexite,
  b.score,
  b.score_motif
FROM ${BDD_TMP}.de_projet_step1 a
FULL OUTER JOIN ${BDD_TMP}.de_projet_score b
ON ( a.id_projet_origine = b.id_projet_origine ) ;

-- Table rapatriant tous les éléments de DE
DROP TABLE IF EXISTS ${BDD_TMP}.projet_de;
CREATE TABLE ${BDD_TMP}.projet_de AS
SELECT concat('001_',a.id_projet)           AS id_projet,
  a.c_type_modele                           AS modele_economique,
  a.n_siren                                 AS cl_siren,
  a.n_siret_etab                            AS cl_etablissement,
  CAST(a.m_ht_financement AS DECIMAL(15,2)) AS mt_financement_prev_ht,
  a.demande_definitive,
  a.code_type_dossier,
  a.code_type_ligne,
  a.code_campagne,
  a.code_produit_financier,
  a.code_detail_prouit,
  a.code_type_produit,
  a.code_lease_back,
  a.code_pool,
  a.d_statut_courant,
  a.ie_utilisateur,
  a.score_libelle,
  a.code_ligne_groupe,
  a.code_score_decision,
  a.code_utilisable,
  a.code_naf,
  a.partenaire_pool_cal,
  a.n_apporteur_principal,
  a.n_apporteur_commissionne,
  a.n_ext_pmp,
  a.n_ext_pmp_agence,
  a.n_ext_pmp_commercial,
  b.modele_marche     AS cl_marche,
  b.modele_population AS modele_population,
  b.complexite,
  b.score       AS score,
  b.score_motif AS score_motif
FROM ${BDD_TMP}.de_dossier_step1 a
LEFT OUTER JOIN ${BDD_TMP}.de_projet_step2 b
ON ( a.id_projet_origine = b.id_projet_origine );

-- Table projet temporaire ajout NFC
DROP TABLE IF EXISTS ${BDD_TMP}.projet_de_nfc;
CREATE TABLE ${BDD_TMP}.projet_de_nfc AS
SELECT
  CASE
    WHEN a.id_projet IS NOT NULL
    THEN a.id_projet
    ELSE c.id_projet
  END AS id_projet,
  a.modele_economique,
  a.cl_siren,
  a.cl_etablissement,
  a.cl_marche,
  a.modele_population,
  a.complexite,
  a.score,
  a.score_motif,
  a.mt_financement_prev_ht,
  a.demande_definitive,
  a.code_type_dossier,
  a.code_type_ligne,
  a.code_campagne,
  a.code_produit_financier,
  a.code_detail_prouit,
  a.code_type_produit,
  a.code_lease_back,
  a.code_pool,
  a.d_statut_courant,
  a.ie_utilisateur,
  a.score_libelle,
  a.code_ligne_groupe,
  a.code_score_decision,
  a.code_utilisable,
  a.code_naf,
  a.partenaire_pool_cal,
  a.n_apporteur_principal,
  a.n_apporteur_commissionne,
  a.n_ext_pmp,
  a.n_ext_pmp_agence,
  a.n_ext_pmp_commercial,
  c.es_crca_conseiller,
  c.es_crca_agence,
  c.es_reseau,
  c.bareme
FROM ${BDD_TMP}.projet_de a
FULL OUTER JOIN ${BDD_TMP}.projet_origine_nfc c
ON ( a.id_projet = c.id_projet );


---  Etape de transformation avec EKIP ---
-- Raptriement de tous les dossiers EKIP avec leur id technique
DROP TABLE IF EXISTS ${BDD_TMP}.element_tmp;
CREATE TABLE ${BDD_TMP}.element_tmp AS
SELECT b.id_projet,
  a.id_element,
  a.id_simul_element,
  a.id_affaire,
  a.no_element
FROM ${BDD_COMMUN}.ekip_element a
LEFT OUTER JOIN ${BDD_TMP}.projet_origine_tmp b
ON (a.id_element = b.id_projet_origine
AND b.id_application="207-EKIP")
;


-- Table intermédiaire servant à calculer le montant de financement CALEF

DROP TABLE IF EXISTS ${BDD_TMP}.element_tmp2;
CREATE TABLE ${BDD_TMP}.element_tmp2 AS
SELECT DISTINCT t2.ID_ELEMENT,
  t1.ID_AFFAIRE,
  t1.IE_AFFAIRE,
  t2.NO_ELEMENT,
  t2.ID_SIMUL_ELEMENT,
  t1.CODE_RESEAU,
  t1.CODE_PRODUIT,
  t1.CODE_STATUT,
  t1.TIERS_SOCIETE,
  t1.TIERS_CLIENT,
  t1.DATE_MEL,
  t2.BASE_LOCATIVE,
  t1.FLAG_POOL,
  t4.NUMERATEUR,
  t4.DENOMINATEUR,
  -- MT_PROD
  CASE
    WHEN flag_pool='O'
    THEN (t2.BASE_LOCATIVE*t4.NUMERATEUR/t4.DENOMINATEUR)
    ELSE t2.BASE_LOCATIVE
  END AS mt_financement_calef_ht
  --  (t2.BASE_LOCATIVE*t4.NUMERATEUR/t4.DENOMINATEUR) AS mt_financement_calef_ht
FROM ${BDD_COMMUN}.EKIP_AFFAIRE t1
INNER JOIN ${BDD_COMMUN}.EKIP_ELEMENT t2
ON (t1.ID_AFFAIRE = t2.ID_AFFAIRE)
INNER JOIN ${BDD_COMMUN}.EKIP_TABECH t3
ON (t2.ID_SIMUL_ELEMENT = t3.ID_SIMUL_ELEMENT)
LEFT OUTER JOIN ${BDD_COMMUN}.EKIP_DETPOOL t4
ON (t1.ID_AFFAIRE      = t4.ID_AFFAIRE)
AND (t1.TIERS_SOCIETE  = t4.TIERS_PARTICIPANT)
WHERE t1.TIERS_SOCIETE = 'B' ;

-- Création de la table pivot EKIP servant à l'agregation des données
DROP TABLE IF EXISTS ${BDD_TMP}.element_simul;
CREATE TABLE ${BDD_TMP}.element_simul AS
SELECT a.id_projet,
  a.id_element,
  a.id_simul_element,
  a.id_affaire,
  (b.taux_nominal)/100 AS tx_nominal_client, -- transformer de pourcentage vers nombre décimal
  b.teg_actuariel /100 AS tx_client,         -- transformer de pourcentage vers nombre décimal
  b.mt_bien_fin_ht     AS mt_financement_ht,
  c.mt_financement_calef_ht
FROM ${BDD_TMP}.element_tmp a
LEFT OUTER JOIN ${BDD_TMP}.element_tmp2 c
ON (a.id_element = c.id_element)
INNER JOIN ${BDD_COMMUN}.ekip_simul b
ON ( a.id_simul_element = b.id_simul_element );




-- Table intermédiaire permettant le rapatriement des éléments provenant de ekip_speeltcbf
DROP TABLE IF EXISTS ${BDD_TMP}.projet_speeltcbf1;
CREATE TABLE ${BDD_TMP}.projet_speeltcbf1 AS
SELECT a.id_projet,
  b.id_element,
  CASE
    WHEN b.taux_fixe IS NOT NULL
    THEN b.taux_fixe/100 						 -- transformer de pourcentage vers nombre décimal
    ELSE 0.0
  END AS tx_refi_crca,
  CASE
    WHEN b.mt_commission_servicing IS NOT NULL
    THEN b.mt_commission_servicing
    ELSE 0.0
  END AS mt_comserv_convention_ht,
  CASE
    WHEN b.taux_commission_servicing IS NOT NULL
    THEN b.taux_commission_servicing/100		  -- transformer de pourcentage vers nombre décimal
    ELSE 0.0
  END AS tx_comserv_convention
FROM ${BDD_TMP}.element_tmp a
INNER JOIN ${BDD_COMMUN}.ekip_speeltcbf b
ON ( a.id_element = b.id_element ) ;

-- Table intermédiaire permettant le rapatriement des montants et intérets
DROP TABLE IF EXISTS ${BDD_TMP}.projet_mt_intrts_cap_rembourse;
CREATE TABLE ${BDD_TMP}.projet_mt_intrts_cap_rembourse AS
SELECT es.id_projet,
  es.id_element,
  SUM(interet) AS mt_interets_client,
  SUM(crb)     AS mt_capital_rembourse_client
FROM ${BDD_TMP}.element_simul es
INNER JOIN ${BDD_COMMUN}.ekip_tabech te
ON (te.id_element =es.id_element
AND te.code_statut='EXPL')
GROUP BY es.id_projet,
  es.id_element;

-- Table intermédiaire permettant le rapatriement des taux de commission de risque
DROP TABLE IF EXISTS ${BDD_TMP}.projet_tx_com_risq;
CREATE TABLE ${BDD_TMP}.projet_tx_com_risq AS
SELECT e.id_projet,
  e.id_element,
  co.ie_convention,
  co.lib_convention,
  AVG(sp.pourcentage)/100 AS tx_commission_risque		-- transformer de pourcentage vers nombre décimal
FROM ${BDD_TMP}.element_simul e
INNER JOIN ${BDD_COMMUN}.ekip_simpres sp
ON (e.id_simul_element=sp.id_simul_element
AND sp.code_statut = 'EXPL' )
INNER JOIN ${BDD_COMMUN}.ekip_convfin cf
ON ( sp.id_convfin = cf.id_convfin )
INNER JOIN ${BDD_COMMUN}.ekip_convention co
ON ( cf.id_convention = co.id_convention )
WHERE co.ie_convention='GARAGLES01'
GROUP BY e.id_projet,
  e.id_element,
  co.ie_convention,
  co.lib_convention ;

-- Table intermédiaire permettant le rapatriement des montants de commission de risque
DROP TABLE IF EXISTS ${BDD_TMP}.projet_mt_com_risq;
CREATE TABLE ${BDD_TMP}.projet_mt_com_risq AS
SELECT es.id_projet,
  es.id_element,
  SUM(c.code_sens_ni * c.mt_ht) AS mt_commission_risque_ht
FROM ${BDD_COMMUN}.ekip_speeltcbf eta
INNER JOIN ${BDD_TMP}.element_simul es
ON ( es.id_element = eta.id_element )
INNER JOIN ${BDD_COMMUN}.ekip_affaire a
ON ( a.id_affaire=es.id_affaire )
INNER JOIN ${BDD_COMMUN}.ekip_simpres sp
ON (es.id_simul_element=sp.id_simul_element
AND sp.code_statut = 'EXPL' )
INNER JOIN ${BDD_COMMUN}.ekip_comarev c
ON (sp.id_simpres = c.id_provenance
AND c.code_statut = 'EXPL' )
INNER JOIN ${BDD_COMMUN}.ekip_convfin cf
ON ( sp.id_convfin = cf.id_convfin )
INNER JOIN ${BDD_COMMUN}.ekip_convention co
ON ( cf.id_convention = co.id_convention )
WHERE co.ie_convention = 'GARAGLES01'
GROUP BY es.id_projet,
  es.id_element ;

-- Table intermédiaire permettant le rapatriement des montants de commission d'apport
DROP TABLE IF EXISTS ${BDD_TMP}.projet_mt_commission_apport;
CREATE TABLE ${BDD_TMP}.projet_mt_commission_apport AS
SELECT e.id_projet,
  e.id_element,
  concat(co.ie_convention,' ', co.lib_convention) AS commission,
  SUM(c.code_sens_ni * c.mt_ht)                   AS mt_commission_apport_ht
FROM ${BDD_TMP}.element_simul e
INNER JOIN ${BDD_COMMUN}.ekip_simpres sp
ON (e.id_simul_element=sp.id_simul_element
AND sp.code_statut = 'EXPL' )
INNER JOIN ${BDD_COMMUN}.ekip_comarev c
ON (sp.id_simpres = c.id_provenance
AND c.code_statut = 'EXPL' )
INNER JOIN ${BDD_COMMUN}.ekip_convfin cf
ON ( sp.id_convfin = cf.id_convfin )
INNER JOIN ${BDD_COMMUN}.ekip_convention co
ON ( cf.id_convention = co.id_convention )
WHERE co.ie_convention='AP CR GLES'
GROUP BY e.id_projet,
  e.id_element,
  co.ie_convention,
  co.lib_convention ;

-- Table rapatriant les élements sur la date échéance depuis tabech
DROP TABLE IF EXISTS ${BDD_TMP}.tabech_events;
CREATE TABLE ${BDD_TMP}.tabech_events AS
SELECT id_element,
  MIN(date_eccheance_8601)                                                                               AS date_debut,
  MAX(date_eccheance_8601)                                                                               AS date_fin,
  months_between(MAX(date_eccheance_8601),MIN(date_eccheance_8601))+1                                    AS nb_mois,
  COUNT(DISTINCT date_echeance)                                                                          AS nb_echeances,
  COUNT(DISTINCT date_echeance)/(months_between(MAX(date_eccheance_8601),MIN(date_eccheance_8601))+1)*12 AS nb_echeances_par_an
FROM ${BDD_COMMUN}.ekip_tabech
WHERE code_statut='EXPL'
GROUP BY id_element;

-- Table intermédiaire permettant d'avoir le premier loyer majoré, le taux sur la base locative et la valeur de rachat du dossier
DROP TABLE IF EXISTS ${BDD_TMP}.tabech_1;
CREATE TABLE ${BDD_TMP}.tabech_1 AS
SELECT
  CASE
    WHEN a.id_element IS NULL
    THEN b.id_element
    ELSE a.id_element
  END                                       AS id_element,
  CAST(a.mt_valeur_rachat AS DECIMAL(15,2)) AS mt_valeur_rachat,
  a.echeance_max,
  b.mt_premier_loyer,
  (b.tx_premier_loyer_base_locative/100) AS 	tx_premier_loyer_base_locative		-- transformer de pourcentage vers nombre décimal
FROM
  (SELECT c.id_element AS id_element,
    c.echeance_max     AS echeance_max,
    d.mt_valeur_rachat AS mt_valeur_rachat
  FROM
    (SELECT id_element,
      MAX(no_echeance) AS echeance_max
    FROM ${BDD_COMMUN}.ekip_tabech
    WHERE code_statut='EXPL'
    GROUP BY id_element
    ) c
  INNER JOIN
    (SELECT id_element,
      mt_valeur_rachat,
      no_echeance
    FROM ${BDD_COMMUN}.ekip_tabech
    WHERE code_statut     ='EXPL'
    AND mt_valeur_rachat IS NOT NULL
    ) d
  ON (c.id_element  = d.id_element
  AND c.echeance_max=d.no_echeance)
  ) a
FULL OUTER JOIN
  (SELECT e.id_element,
    t.no_echeance,
    t.interet                                AS interet,
    t.crb                                    AS crb,
    e.base_locative                          AS base_locative,
    CAST((t.interet +t.crb) AS                  DECIMAL(15,2)) AS mt_premier_loyer,
    CAST(((t.interet+t.CRB)/e.base_locative) AS DECIMAL(15,6)) AS tx_premier_loyer_base_locative,
    t.code_type_element,
    t.code_statut
  FROM ${BDD_COMMUN}.ekip_element e
  INNER JOIN ${BDD_COMMUN}.ekip_tabech t
  ON (e.id_element = t.id_element)
  WHERE ( t.no_echeance   = 1
  AND t.code_type_element = 'LOYE'
  AND t.ID_MVT           <> -1
  AND t.code_statut       ='EXPL')
  ) b ON ( a.id_element   = b.id_element ) ;
