
DROP TABLE ${BDD_LEASING_DATA_TMP}.agr_tiers_tmp1;

#@(#) -----------------------------------------------------------------------------------
#@(#) Nom              : trans_projet.sql
#@(#) Auteur           : capgemini
#@(#) Date de creation : 06/12/2017
#@(#) -----------------------------------------------------------------------------------
#@(#) Description      : Alimentation table projet
#@(#) -----------------------------------------------------------------------------------
#@(#) Historique des modifications
#@(#) 06/12/2017 - SKO - création
#@(#) -----------------------------------------------------------------------------------


set hive.exec.parallel=true;


-- Rapatriement des derniers scores calculés d'un projet DE

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.de_projet_score_step1;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.de_projet_score_step1 AS
SELECT n_numero_dossier,
  MAX(n_demande) AS n_demande
FROM ${BDD_COMMUN}.de_reponse_octroi
GROUP BY n_numero_dossier ;

-- Table temporaire permettant la récupération des scores d'un projet DE
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.de_projet_score;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.de_projet_score AS
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
FROM ${BDD_LEASING_DATA_TMP}.de_projet_score_step1 a
LEFT OUTER JOIN ${BDD_COMMUN}.de_reponse_octroi b
ON (a.n_numero_dossier = b.n_numero_dossier
AND a.n_demande = b.n_demande);

-- Table temporaire permettant la récupération des types de population d'un projet DE
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.de_projet_population;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.de_projet_population AS
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
FROM ${BDD_LEASING_DATA_TMP}.de_dossier_step1 a
LEFT OUTER JOIN ${BDD_COMMUN}.de_nomenclature b
ON ( a.c_provenance = b.c_nomenclature
AND b.c_type_nomenclature = 'PRDO' );

-- Table temporaire permettant la récupération des modèles de marché d'un projet DE
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.de_projet_marche;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.de_projet_marche AS
SELECT a.id_projet_origine,
  b.l_libelle AS modele_marche
FROM ${BDD_LEASING_DATA_TMP}.de_dossier_step1 a
LEFT OUTER JOIN ${BDD_COMMUN}.de_nomenclature b
ON ( a.c_marche_ccf = b.c_nomenclature
AND b.c_type_nomenclature = 'MCCF' );

-- Table temporaire stockant les modèle de marché et de population des dossiers DE
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.de_projet_step1;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.de_projet_step1 AS
SELECT
  CASE
    WHEN a.id_projet_origine IS NULL
    THEN b.id_projet_origine
    ELSE a.id_projet_origine
  END AS id_projet_origine,
  b.modele_marche,
  a.modele_population,
  a.complexite
FROM ${BDD_LEASING_DATA_TMP}.de_projet_population a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.de_projet_marche b
ON ( a.id_projet_origine = b.id_projet_origine );

-- Table temporaire stockant les modèle de marché, de population et le score des dossiers DE
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.de_projet_step2;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.de_projet_step2 AS
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
FROM ${BDD_LEASING_DATA_TMP}.de_projet_step1 a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.de_projet_score b
ON ( a.id_projet_origine = b.id_projet_origine ) ;

-- Table rapatriant tous les éléments de DE
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_de;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_de AS
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
FROM ${BDD_LEASING_DATA_TMP}.de_dossier_step1 a
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.de_projet_step2 b
ON ( a.id_projet_origine = b.id_projet_origine );

-- Table projet temporaire ajout NFC
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_de_nfc;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_de_nfc AS
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
FROM ${BDD_LEASING_DATA_TMP}.projet_de a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_origine_nfc c
ON ( a.id_projet = c.id_projet );


---  Etape de transformation avec EKIP ---
-- Raptriement de tous les dossiers EKIP avec leur id technique
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.element_tmp;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.element_tmp AS
SELECT b.id_projet,
  a.id_element,
  a.id_simul_element,
  a.id_affaire,
  a.no_element
FROM ${BDD_COMMUN}.ekip_element a
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_origine_tmp b
ON (a.id_element = b.id_projet_origine
AND b.id_application="207-EKIP")
;


-- Table intermédiaire servant à calculer le montant de financement CALEF

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.element_tmp2;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.element_tmp2 AS
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
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.element_simul;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.element_simul AS
SELECT a.id_projet,
  a.id_element,
  a.id_simul_element,
  a.id_affaire,
  (b.taux_nominal)/100 AS tx_nominal_client, -- transformer de pourcentage vers nombre décimal
  b.teg_actuariel /100 AS tx_client,         -- transformer de pourcentage vers nombre décimal
  b.mt_bien_fin_ht     AS mt_financement_ht,
  c.mt_financement_calef_ht
FROM ${BDD_LEASING_DATA_TMP}.element_tmp a
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.element_tmp2 c
ON (a.id_element = c.id_element)
INNER JOIN ${BDD_COMMUN}.ekip_simul b
ON ( a.id_simul_element = b.id_simul_element );




-- Table intermédiaire permettant le rapatriement des éléments provenant de ekip_speeltcbf
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_speeltcbf1;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_speeltcbf1 AS
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
FROM ${BDD_LEASING_DATA_TMP}.element_tmp a
INNER JOIN ${BDD_COMMUN}.ekip_speeltcbf b
ON ( a.id_element = b.id_element ) ;

-- Table intermédiaire permettant le rapatriement des montants et intérets
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_mt_intrts_cap_rembourse;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_mt_intrts_cap_rembourse AS
SELECT es.id_projet,
  es.id_element,
  SUM(interet) AS mt_interets_client,
  SUM(crb)     AS mt_capital_rembourse_client
FROM ${BDD_LEASING_DATA_TMP}.element_simul es
INNER JOIN ${BDD_COMMUN}.ekip_tabech te
ON (te.id_element =es.id_element
AND te.code_statut='EXPL')
GROUP BY es.id_projet,
  es.id_element;

-- Table intermédiaire permettant le rapatriement des taux de commission de risque
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_tx_com_risq;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_tx_com_risq AS
SELECT e.id_projet,
  e.id_element,
  co.ie_convention,
  co.lib_convention,
  AVG(sp.pourcentage)/100 AS tx_commission_risque		-- transformer de pourcentage vers nombre décimal
FROM ${BDD_LEASING_DATA_TMP}.element_simul e
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
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_mt_com_risq;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_mt_com_risq AS
SELECT es.id_projet,
  es.id_element,
  SUM(c.code_sens_ni * c.mt_ht) AS mt_commission_risque_ht
FROM ${BDD_COMMUN}.ekip_speeltcbf eta
INNER JOIN ${BDD_LEASING_DATA_TMP}.element_simul es
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
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_mt_commission_apport;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_mt_commission_apport AS
SELECT e.id_projet,
  e.id_element,
  concat(co.ie_convention,' ', co.lib_convention) AS commission,
  SUM(c.code_sens_ni * c.mt_ht)                   AS mt_commission_apport_ht
FROM ${BDD_LEASING_DATA_TMP}.element_simul e
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
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.tabech_events;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.tabech_events AS
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
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.tabech_1;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.tabech_1 AS
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

-- Jointure entre tabech_1 et tabech_events
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.tabech_final;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.tabech_final AS
SELECT
  CASE
    WHEN a.id_element IS NULL
    THEN b.id_element
    ELSE a.id_element
  END AS id_element,
  a.mt_valeur_rachat,
  a.mt_premier_loyer,
  a.tx_premier_loyer_base_locative,
  b.nb_mois,
  b.nb_echeances,
  b.nb_echeances_par_an
FROM ${BDD_LEASING_DATA_TMP}.tabech_1 a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.tabech_events b
ON ( a.id_element = b.id_element );

-- Première table temporaire EKIP permettant de rapatrier les informations contenues dans element_simul , projet_speeltcbf1
-- tabech_final
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip1;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip1 AS
SELECT DISTINCT
  CASE
    WHEN ekip1.id_projet IS NULL
    THEN ekip5.id_projet
    ELSE ekip1.id_projet
  END AS id_projet,
  ekip1.tx_nominal_client,
  ekip1.tx_client,
  ekip1.mt_financement_ht,
  ekip1.mt_financement_calef_ht,
  ekip1.tx_refi_crca,
  ekip1.mt_comserv_convention_ht,
  ekip1.tx_comserv_convention,
  ekip1.tx_marge_financiere,
  ekip5.mt_valeur_rachat,
  ekip5.mt_premier_loyer,
  ekip5.tx_premier_loyer_base_locative,
  ekip5.nb_mois,
  ekip5.nb_echeances,
  ekip5.nb_echeances_par_an
FROM
  ( SELECT DISTINCT a.id_projet,
    a.tx_nominal_client,
    a.tx_client,
    a.mt_financement_ht,
    a.mt_financement_calef_ht,
    b.tx_refi_crca,
    b.mt_comserv_convention_ht,
    b.tx_comserv_convention,
    (a.tx_nominal_client - b.tx_refi_crca ) AS tx_marge_financiere
  FROM ${BDD_LEASING_DATA_TMP}.element_simul a
  LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_speeltcbf1 b
  ON ( a.id_projet = b.id_projet )
  ) ekip1
FULL OUTER JOIN
  (SELECT a.id_projet,
    b.mt_valeur_rachat,
    b.mt_premier_loyer,
    b.tx_premier_loyer_base_locative,
    b.nb_mois,
    b.nb_echeances,
    b.nb_echeances_par_an
  FROM ${BDD_LEASING_DATA_TMP}.element_tmp a
  LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tabech_final b -- Réferencement des éléments de tabech_final avec un id_projet DATUM à partir de ekip_element
  ON ( a.id_element = b.id_element )
  ) ekip5 ON (ekip1.id_projet = ekip5.id_projet);

-- Deuxième table temporaire EKIP permettant de rapatrier les informations contenues dans projet_tx_com_risq,
-- projet_mt_intrts_cap_rembours + projet_mt_com_risq et projet_mt_commission_apport
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip2;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip2 AS
SELECT
  CASE
    WHEN ekip2.id_projet IS NULL
    THEN ekip3.id_projet
    ELSE ekip2.id_projet
  END AS id_projet,
  ekip2.tx_commission_risque,
  ekip2.mt_interets_client,
  ekip2.mt_capital_rembourse_client,
  ekip3.mt_commission_risque_ht,
  ekip3.mt_commission_apport_ht
FROM
  ( SELECT DISTINCT
    CASE
      WHEN b.id_projet IS NULL
      THEN a.id_projet
      ELSE b.id_projet
    END AS id_projet,
    a.tx_commission_risque,
    b.mt_interets_client,
    b.mt_capital_rembourse_client
  FROM ${BDD_LEASING_DATA_TMP}.projet_tx_com_risq a
  FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_mt_intrts_cap_rembourse b -- (jointure entre projet_tx_com_risq et projet_mt_intrts_cap_rembourse)
  ON ( a.id_projet = b.id_projet )
  ) ekip2
FULL JOIN
  ( SELECT DISTINCT
    CASE
      WHEN a.id_projet IS NULL
      THEN b.id_projet
      ELSE a.id_projet
    END AS id_projet,
    a.mt_commission_risque_ht,
    b.mt_commission_apport_ht
  FROM ${BDD_LEASING_DATA_TMP}.projet_mt_com_risq a
  FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_mt_commission_apport b --(jointure entre projet_mt_commission_risq et projet_mt_commision_apport):
  ON (a.id_projet = b.id_projet)
  ) ekip3 ON (ekip2.id_projet = ekip3.id_projet);

--Table intermédiaire permettant le rapatriement des montants frais dossier client(sprint5)
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_mt_frais_dossier_client;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_mt_frais_dossier_client AS
SELECT t2.id_projet AS id_projet,
  t1.mt_frais_dossier_client
FROM
  (SELECT t.id_element,
    CAST(SUM(mn.code_sens_ni * mln.mt_ht) AS DECIMAL(15,2)) AS mt_frais_dossier_client
  FROM ${BDD_COMMUN}.ekip_tabech t
  INNER JOIN ${BDD_COMMUN}.ekip_mvtnew mn
  ON ( t.id_mvt = mn.id_mvt
  AND mn.code_statut='2' )
  INNER JOIN ${BDD_COMMUN}.ekip_MVTLIGNEW mln
  ON ( mn.id_mvt = mln.id_mvt
  AND mln.code_statut='2' )
  INNER JOIN ${BDD_COMMUN}.ekip_fluxprev f
  ON ( f.id_fluxprev = mln.id_provenance
  AND f.id_element=t.id_element ) -- resserer la selection pour prendre en compte les ADJOINCTIONS (ex : id_element = 1811640728)
  INNER JOIN ${BDD_COMMUN}.ekip_typligne tl
  ON ( mn.code_operation = tl.code_operation
  AND mln.code_type_ligne=tl.code_type_ligne
  AND mln.code_nat_ligne =tl.code_nature_ligne )
  WHERE tl.libelle = 'Frais de dossier avec échéance' -- à traiter également (autre US) 'Frais de formalité'
  GROUP BY t.id_element
  ) t1
INNER JOIN
  (SELECT t.id_element,
    e.id_projet
  FROM ${BDD_COMMUN}.ekip_tabech t
  INNER JOIN ${BDD_LEASING_DATA_TMP}.element_tmp e
  ON ( t.id_element = e.id_element )
  ) t2 ON (t1.id_element = t2.id_element);

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_mt_frais_crca_pnb;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_mt_frais_crca_pnb AS
SELECT DISTINCT
  CASE
    WHEN t1.id_projet IS NULL
    THEN t2.id_projet
    ELSE t1.id_projet
  END AS id_projet,
  t1.mt_frais_dossier_crca, -- rapatriement des montants frais frais dossier crca(sprint5)
  t2.mt_pnb_ht
FROM
  (SELECT e.id_element,
    e.id_projet,
    CAST(SUM(sp.montant_prime) AS DECIMAL(15,2)) AS mt_frais_dossier_crca
  FROM ${BDD_LEASING_DATA_TMP}.element_tmp e
  INNER JOIN ${BDD_COMMUN}.ekip_simpres sp
  ON ( sp.id_simul_element = e.id_simul_element
  AND sp.code_statut ='EXPL' )
  INNER JOIN ${BDD_COMMUN}.ekip_convfin cf
  ON ( cf.id_convfin = sp.id_convfin )
  INNER JOIN ${BDD_COMMUN}.ekip_typligne tl
  ON ( tl.code_type_ligne = cf.code_type_ligne
  AND tl.code_operation = cf.code_operation
  AND tl.code_nature_ligne = cf.code_nat_ligne )
  WHERE tl.libelle = 'VERSEMENT FRAIS DOSSIER CRCA' -- code_operation FACD code_nature_ligne VCOM code_type_ligne FREV
  GROUP BY e.id_projet,
    e.id_element
  ) t1
FULL OUTER JOIN
  ( -- calcul de PNB des opérations CBM pour sur tous les contrats quelque soit l'apporteur(sprint6)
  SELECT tb1.id_projet,
    tb1.id_element,
    CAST(SUM(mt_ht) AS DECIMAL(15,2)) AS mt_pnb_ht
  FROM
    (SELECT e.id_projet,
      e.id_element,
      SUM(interet) mt_ht,
      COUNT(e.id_element) nombre,
      'INTERETS' flux
    FROM ${BDD_LEASING_DATA_TMP}.element_tmp e
    INNER JOIN ${BDD_COMMUN}.ekip_simul s
    ON (e.id_simul_element=s.id_simul_element)
    INNER JOIN ${BDD_COMMUN}.ekip_tabech te
    ON (te.id_element = e.id_element
    AND te.code_statut='EXPL')
    GROUP BY e.id_projet,
      e.id_element
    UNION
    SELECT e.id_projet,
      f.id_element ,
      SUM(mt_ht) mt_ht ,
      COUNT(f.no_element) nombre ,
      libelle flux
    FROM ${BDD_COMMUN}.ekip_fluxprev f
    INNER JOIN ${BDD_LEASING_DATA_TMP}.element_tmp e
    ON ( f.id_element = e.id_element )
    GROUP BY e.id_projet,
      f.id_element,
      LIBELLE
    ) tb1
  GROUP BY tb1.id_element,
    tb1.id_projet
  ) t2 ON (t1.id_projet = t2.id_projet) ;

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_code_offre_marge;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_code_offre_marge AS
SELECT DISTINCT
  CASE
    WHEN t1.id_projet IS NULL
    THEN t2.id_projet
    ELSE t1.id_projet
  END AS id_projet,
  t1.code_offre,
  t1.code_produit,
  t1.id_affaire,
  t1.ie_affaire,
  t1.no_element,
  t2.mt_marge_commerciale,
  t2.mt_marge_commerciale_bonifiee,
  t2.mt_marge_frontale,
  t2.mt_marge_frontale_bonifiee
FROM
  (SELECT e.id_projet,
    t1.id_affaire,
    t1.ie_affaire, -- Rapatriement de l'ie_affaire pour la recherche d'un projet par affaire
    e.no_element, -- Rapatriement du no_element pour une affaire
    CASE t1.code_offre
      WHEN 'null'
      THEN NULL
      ELSE t1.code_offre
    END AS code_offre, --Rapatriement de la colonne code_offre qui represente le code_offre pour les differents elements EKIP
    t1.code_produit    -- ajout du code produit
  FROM ${BDD_COMMUN}.ekip_affaire t1
  INNER JOIN ${BDD_LEASING_DATA_TMP}.element_tmp e
  ON ( e.id_affaire = t1.id_affaire )
  ) t1
FULL OUTER JOIN
  ( -- Calcul des marge commerciales et frontales
  SELECT DISTINCT e.id_projet,
    tr.id_element,
    CAST(marge_commerciale_montant AS  DECIMAL(15,2)) AS mt_marge_commerciale,
    CAST(marge_commerciale_bonifiee AS DECIMAL(15,2)) AS mt_marge_commerciale_bonifiee,
    CAST(marge_frontale_montant AS     DECIMAL(15,2)) AS mt_marge_frontale,
    CAST(marge_frontale_bonifiee AS    DECIMAL(15,2)) AS mt_marge_frontale_bonifiee
  FROM ${BDD_COMMUN}.ekip_tri_resultats tr
  INNER JOIN ${BDD_LEASING_DATA_TMP}.element_tmp e
  ON ( e.id_element = tr.id_element )
  ) t2 ON (t1.id_projet = t2.id_projet) ;

-- Toisième table temporaire EKIP permettant de rapatrier les informations contenues dans projet_mt_frais_dossier_client,
-- projet_mt_frais_crca_pnb et agrégeant aussi les données provenant des 2 premières tables temporaires
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip3;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip3 AS
SELECT DISTINCT
  CASE
    WHEN t1.id_projet IS NULL
    THEN t2.id_projet
    ELSE t1.id_projet
  END AS id_projet,
  t1.tx_nominal_client,
  t1.tx_client,
  t1.mt_financement_ht,
  t1.mt_financement_calef_ht,
  t1.tx_refi_crca,
  t1.mt_comserv_convention_ht,
  t1.tx_comserv_convention,
  t1.tx_marge_financiere,
  t1.mt_valeur_rachat,
  t1.mt_premier_loyer,
  t1.tx_premier_loyer_base_locative,
  t1.nb_mois,
  t1.nb_echeances,
  t1.nb_echeances_par_an,
  t1.tx_commission_risque,
  t1.mt_interets_client,
  t1.mt_capital_rembourse_client,
  t1.mt_commission_risque_ht,
  t1.mt_commission_apport_ht,
  ((t1.mt_interets_client)/t1.tx_nominal_client) AS mt_encours_cumule_moyen,
  t2.mt_frais_dossier_client,
  t2.mt_frais_dossier_crca,
  t2.mt_pnb_ht
FROM
  (SELECT
    CASE
      WHEN ekip1.id_projet IS NULL
      THEN ekip2.id_projet
      ELSE ekip1.id_projet
    END AS id_projet,
    ekip1.tx_nominal_client,
    ekip1.tx_client,
    ekip1.mt_financement_ht,
    ekip1.mt_financement_calef_ht,
    ekip1.tx_refi_crca,
    ekip1.mt_comserv_convention_ht,
    ekip1.tx_comserv_convention,
    ekip1.tx_marge_financiere,
    ekip1.mt_valeur_rachat,
    ekip1.mt_premier_loyer,
    ekip1.tx_premier_loyer_base_locative,
    ekip1.nb_mois,
    ekip1.nb_echeances,
    ekip1.nb_echeances_par_an,
    ekip2.tx_commission_risque,
    ekip2.mt_interets_client,
    ekip2.mt_capital_rembourse_client,
    ekip2.mt_commission_risque_ht,
    ekip2.mt_commission_apport_ht
  FROM ${BDD_LEASING_DATA_TMP}.projet_ekip1 ekip1
  FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_ekip2 ekip2
  ON ( ekip1.id_projet = ekip2.id_projet )
  ) t1
FULL OUTER JOIN
  (SELECT
    CASE
      WHEN a.id_projet IS NULL
      THEN b.id_projet
      ELSE a.id_projet
    END AS id_projet,
    a.mt_frais_dossier_client,
    b.mt_frais_dossier_crca,
    b.mt_pnb_ht
  FROM ${BDD_LEASING_DATA_TMP}.projet_mt_frais_dossier_client a
  FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_mt_frais_crca_pnb b
  ON ( a.id_projet = b.id_projet )
  ) t2 ON ( t1.id_projet = t2.id_projet ) ;




-- Table temporaire récuprérant toutes les données d'EKIP nécessaires à l'enrichissement du dataset final
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip4;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip4 AS
SELECT DISTINCT
  CASE
    WHEN a.id_projet IS NULL
    THEN b.id_projet
    ELSE a.id_projet
  END AS id_projet,
  b.code_offre,
  b.code_produit,
  b.ie_affaire,
  b.no_element,
  CAST(a.mt_capital_rembourse_client AS DECIMAL(15,2)) AS mt_capital_rembourse_client,
  CAST(a.mt_commission_apport_ht AS     DECIMAL(15,2)) AS mt_commission_apport_ht,
  CAST(a.mt_commission_risque_ht AS     DECIMAL(15,2)) AS mt_commission_risque_ht,
  CAST(a.mt_comserv_convention_ht AS  DECIMAL(15,2)) AS mt_comserv_convention_ht,
  CAST(a.mt_encours_cumule_moyen AS     DECIMAL(15,2)) AS mt_encours_cumule_moyen,
  CAST(a.mt_financement_ht AS           DECIMAL(15,2)) AS mt_financement_ht,
  CAST(a.mt_financement_calef_ht AS       DECIMAL(15,2)) AS mt_financement_calef_ht,
  a.mt_frais_dossier_client,
  a.mt_frais_dossier_crca,
  CAST(a.mt_interets_client AS DECIMAL(15,2)) AS mt_interets_client,
  b.mt_marge_commerciale,
  b.mt_marge_commerciale_bonifiee,
  b.mt_marge_frontale,
  b.mt_marge_frontale_bonifiee,
  a.mt_pnb_ht,
  a.mt_premier_loyer,
  a.mt_valeur_rachat,
  CAST(a.nb_mois AS                 DECIMAL(10,2)) AS nb_mois,
  CAST(a.nb_echeances AS            DECIMAL(10,2)) AS nb_echeances,
  CAST(a.nb_echeances_par_an AS     DECIMAL(10,2)) AS nb_echeances_par_an,
  CAST(a.tx_commission_risque AS    DECIMAL(15,6)) AS tx_commission_risque,
  CAST(a.tx_comserv_convention AS DECIMAL(15,6)) AS tx_comserv_convention,
  CAST(a.tx_marge_financiere AS     DECIMAL(15,6)) AS tx_marge_financiere,
  CAST(a.tx_nominal_client AS       DECIMAL(15,6)) AS tx_nominal_client,
  CAST(a.tx_client AS               DECIMAL(15,6)) AS tx_client,
  a.tx_premier_loyer_base_locative,
  CAST(a.tx_refi_crca AS DECIMAL(15,6)) AS tx_refi_crca
FROM ${BDD_LEASING_DATA_TMP}.projet_ekip3 a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_code_offre_marge b -- ajout des données rapatriées dans la table projet_code_offre_marge
ON (a.id_projet = b.id_projet) ;

--table intermediaire pour récuperer la colonne es_crca_region de la table nfc_element_structure
--drop table projet_es_crca_region;
--create table projet_es_crca_region as select
--f.id_element_structure, t.id_element, t.id_projet
--from
--(select distinct
-- t.tiers as ie_element_structure, 'CRCA' as type_element_structure, t.nom as element_structure, e.id_element, e.id_projet
--from air_data_tmp_dev.element_tmp e
--join ${BDD_COMMUN}.ekip_affaire a on e.id_affaire=a.id_affaire
--join  ${BDD_COMMUN}.ekip_reseau_crca_0eta rc on a.code_reseau=rc.code_reseau
--join ${BDD_COMMUN}.ekip_tiers t on t.tiers=rc.code_tiers_reseau) t
--left join
--NFC_ELEMENT_STRUCTURE f
--on (f.ie_element_structure= t.ie_element_structure);
-----

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip5;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip5 AS
SELECT DISTINCT
  --case
  --when t2.id_projet is null then t1.id_projet else t2.id_projet
  --end
  --as
  t1.id_projet,
  t1.code_offre,
  t1.code_produit,
  t1.ie_affaire,
  t1.no_element,
  t1.mt_capital_rembourse_client,
  t1.mt_commission_apport_ht,
  t1.mt_commission_risque_ht,
  t1.mt_comserv_convention_ht,
  t1.mt_encours_cumule_moyen,
  t1.mt_financement_ht,
  t1.mt_financement_calef_ht,
  t1.mt_frais_dossier_client,
  t1.mt_frais_dossier_crca,
  t1.mt_interets_client,
  t1.mt_marge_commerciale,
  t1.mt_marge_commerciale_bonifiee,
  t1.mt_marge_frontale,
  t1.mt_marge_frontale_bonifiee,
  t1.mt_pnb_ht,
  t1.mt_premier_loyer,
  t1.mt_valeur_rachat,
  t1.nb_mois,
  t1.nb_echeances,
  t1.nb_echeances_par_an,
  t1.tx_commission_risque,
  t1.tx_comserv_convention,
  t1.tx_marge_financiere,
  t1.tx_nominal_client,
  t1.tx_client,
  t1.tx_premier_loyer_base_locative,
  t1.tx_refi_crca,
  --t2.id_element_structure as es_crca_region
  'null' AS es_crca_region
FROM ${BDD_LEASING_DATA_TMP}.projet_ekip4 t1
  --full join projet_es_crca_region t2
  --on (t2.id_projet = t1.id_projet)
  ;
-----
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_immobilisation; -- PR-OLU: normer la table d'enrichissement projet_ekip_eltimm_immob
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_immobilisation AS
SELECT source.id_element,
  MIN(i.immobilisation) immobilisation
FROM
  (SELECT id_element,
    MAX(montant) montant
  FROM ${BDD_COMMUN}.ekip_eltimm t3
  INNER JOIN ${BDD_COMMUN}.ekip_immob i
  ON (t3.immobilisation=i.immobilisation)
  GROUP BY id_element
  ) AS source
INNER JOIN ${BDD_COMMUN}.ekip_eltimm t3
ON (t3.id_element=source.id_element)
INNER JOIN ${BDD_COMMUN}.ekip_immob i
ON (t3.immobilisation=i.immobilisation
AND i.montant = source.montant)
GROUP BY source.id_element;

-------

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_bien_principal;-- PR-OLU: normer la table d'enrichissement projet_ekip_eltimm_immob (nécessite un ALTER TABLE -> _tmp)
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_bien_principal AS
SELECT e.id_element,
  e.id_projet,
  i.designation_1 AS bien_designation,
  la1.cod_interne AS bien_nature_code,
  la1.libelle     AS bien_nature,
  la2.libelle     AS bien_categorie
FROM ${BDD_LEASING_DATA_TMP}.projet_immobilisation pi
INNER JOIN ${BDD_COMMUN}.ekip_immob i
ON (i.immobilisation=pi.immobilisation)
INNER JOIN ${BDD_COMMUN}.ekip_LIB_ACODIFS la1
ON (i.code_assiette_theorique = la1.cod_interne
AND la1.typ_code = 'ASCT' )
INNER JOIN ${BDD_COMMUN}.ekip_LIB_ACODIFS la2
ON (i.categorie_immob = la2.CODE
AND la2.typ_code = 'CAIN' )
INNER JOIN ${BDD_LEASING_DATA_TMP}.element_tmp e
ON (e.id_element = pi.id_element);

-- Table intermediaire permettant savoir à quelle assurance est eligible une nature de bien

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.eligibilite_nature_bien; -- PR-OLU: normer la table d'enrichissement "projet_de_natures_assurances_elligibilite"
CREATE TABLE ${BDD_LEASING_DATA_TMP}.eligibilite_nature_bien AS
SELECT DISTINCT t1.ekip,
  t1.id_nat,
  IF(t2.c_cat_materiel      != 'null',1,0) AS assurance_bdm_eligibilite,
  IF(t2.c_typo_materiel_pfi !='null',1,0)  AS assurance_pfi_eligibilite,
  IF(t2.c_typo_materiel_tfi != 'null',1,0) AS assurance_adi_eligibilite
FROM ${BDD_COMMUN}.DE_SAF_NATURES t1 -- PR-OLU: proposition : partir de tous les éléments (element_tmp) de façon à avoir une valeur par défaut à 0 pour tous les éléments non éligibles à l'assurance
INNER JOIN ${BDD_COMMUN}.DE_SAF_ASSURANCES t2
ON (t1.id_nat = t2.id_nat);


-- PR-OLU: normer la table d'enrichissement "projet_de_natures_assurances_elligibilite" (nécessite un ALTER TABLE -> _tmp)
-- OU si la table précédente part de la liste des éléments, supprimer cette table
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_bien_principal2;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_bien_principal2 AS
SELECT t1.id_element,
  t1.id_projet,
  t1.bien_designation,
  t1.bien_nature_code,
  t1.bien_nature,
  t1.bien_categorie,
  t2.assurance_bdm_eligibilite,
  t2.assurance_pfi_eligibilite,
  t2.assurance_adi_eligibilite
FROM ${BDD_LEASING_DATA_TMP}.projet_bien_principal t1
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.eligibilite_nature_bien t2
ON (t1.bien_nature_code=t2.ekip);

--------------

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip6; -- TODO: externaliser l'étape finale d'enrichissement
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip6 AS
SELECT DISTINCT t1.id_projet, -- PR-OLU: préférer t1.* et supprimer la recopie des champs
  t1.code_offre,
  t1.code_produit,
  t1.ie_affaire,
  t1.no_element,
  t1.mt_capital_rembourse_client,
  t1.mt_commission_apport_ht,
  t1.mt_commission_risque_ht,
  t1.mt_comserv_convention_ht,
  t1.mt_encours_cumule_moyen,
  t1.mt_financement_ht,
  t1.mt_financement_calef_ht,
  t1.mt_frais_dossier_client,
  t1.mt_frais_dossier_crca,
  t1.mt_interets_client,
  t1.mt_marge_commerciale,
  t1.mt_marge_commerciale_bonifiee,
  t1.mt_marge_frontale,
  t1.mt_marge_frontale_bonifiee,
  t1.mt_pnb_ht,
  t1.mt_premier_loyer,
  t1.mt_valeur_rachat,
  t1.nb_mois,
  t1.nb_echeances,
  t1.nb_echeances_par_an,
  t1.tx_commission_risque,
  t1.tx_comserv_convention,
  t1.tx_marge_financiere,
  t1.tx_nominal_client,
  t1.tx_client,
  t1.tx_premier_loyer_base_locative,
  t1.tx_refi_crca,
  t1.es_crca_region,
  t2.bien_designation,
  t2.bien_nature_code,
  t2.bien_nature,
  t2.bien_categorie,
  t2.assurance_bdm_eligibilite,
  t2.assurance_pfi_eligibilite,
  t2.assurance_adi_eligibilite
FROM ${BDD_LEASING_DATA_TMP}.projet_ekip5 t1
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_bien_principal2 t2
ON (t1.id_projet = t2.id_projet);

-- Tables intermediaires permettant de calculer les FLAGS utiles pour le calcul du taux d'équipement BDM PFI ADI
--BDM + PFI

-- PR-OLU: distinguer en 3 tables d'enrichissement et normer "projet_ekip_clipres_bdm" "projet_ekip_clipres_adi" "projet_ekip_clipres_pfi"
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_flag_souscription_equip_1;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_flag_souscription_equip_1 AS
SELECT CAST(
  CASE
    WHEN tb1.id_element IS NOT NULL
    THEN tb1.id_element
    ELSE tb2.id_element
  END AS STRING) AS id_element,
  CAST(
  CASE
    WHEN tb1.id_projet IS NOT NULL
    THEN tb1.id_projet
    ELSE tb2.id_projet
  END AS STRING)                                    AS id_projet,
  CAST(IF(assurance_bdm_souscription=1,1,0) AS INT) AS assurance_bdm_souscription,
  CAST(IF(assurance_pfi_souscription=1,1,0) AS INT) AS assurance_pfi_souscription
FROM
  (SELECT e.id_projet,
    e.id_element,
    COUNT(id_element) AS nb,
    1                 AS assurance_bdm_souscription
  FROM ${BDD_LEASING_DATA_TMP}.element_tmp e
  JOIN ${BDD_COMMUN}.ekip_simpres sp
  ON ( sp.id_simul_element=e.id_simul_element )
  INNER JOIN ${BDD_COMMUN}.ekip_clipres cp
  ON (sp.id_clipres=cp.id_clipres)
  INNER JOIN ${BDD_COMMUN}.ekip_startpres_0eta spe
  ON (cp.no_contrat_prestation=spe.no_contrat_prestation)
  WHERE spe.type_start IN ('BDM')
  GROUP BY e.id_projet,
    e.id_element
  ) tb1
FULL OUTER JOIN
  (SELECT e.id_projet,
    e.id_element,
    COUNT(id_element) AS nb,
    1                 AS assurance_pfi_souscription
  FROM ${BDD_LEASING_DATA_TMP}.element_tmp e
  INNER JOIN ${BDD_COMMUN}.ekip_simpres sp
  ON (sp.ID_SIMUL_ELEMENT=e.ID_SIMUL_ELEMENT)
  INNER JOIN ${BDD_COMMUN}.ekip_clipres cp
  ON (sp.id_clipres=cp.id_clipres)
  INNER JOIN ${BDD_COMMUN}.ekip_startpres_0eta spe
  ON (cp.no_contrat_prestation=spe.no_contrat_prestation)
  WHERE spe.type_start IN ('PFI')
  GROUP BY e.id_projet,
    e.id_element
  ORDER BY nb DESC
  ) tb2 ON (tb1.id_element = tb2.id_element) ;

--(BDM + PFI) + ADI

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_flag_souscription_equip_2;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_flag_souscription_equip_2 AS
SELECT CAST(
  CASE
    WHEN tb1.id_element IS NOT NULL
    THEN tb1.id_element
    ELSE tb2.id_element
  END AS STRING) AS id_element,
  CAST(
  CASE
    WHEN tb1.id_projet IS NOT NULL
    THEN tb1.id_projet
    ELSE tb2.id_projet
  END AS STRING)                                    AS id_projet,
  CAST(IF(assurance_bdm_souscription=1,1,0) AS INT) AS assurance_bdm_souscription,
  CAST(IF(assurance_pfi_souscription=1,1,0) AS INT) AS assurance_pfi_souscription,
  CAST(IF(assurance_adi_souscription=1,1,0) AS INT) AS assurance_adi_souscription
FROM
  (SELECT e.id_projet,
    e.id_element,
    assurance_bdm_souscription,
    assurance_pfi_souscription
  FROM ${BDD_LEASING_DATA_TMP}.projet_flag_souscription_equip_1 e
  ) tb1
FULL OUTER JOIN
  (SELECT e.id_projet,
    e.id_element,
    COUNT(id_element) AS nb,-- OLU: usage à échanger ;-)
    1                 AS assurance_adi_souscription
  FROM ${BDD_LEASING_DATA_TMP}.element_tmp e
  INNER JOIN ${BDD_COMMUN}.ekip_simpres sp
  ON (sp.ID_SIMUL_ELEMENT=e.ID_SIMUL_ELEMENT)
  INNER JOIN ${BDD_COMMUN}.ekip_clipres cp
  ON (sp.id_clipres=cp.id_clipres)
  INNER JOIN ${BDD_COMMUN}.ekip_startpres_0eta spe
  ON (cp.no_contrat_prestation=spe.no_contrat_prestation)
  WHERE spe.type_start IN ('ADI')
  GROUP BY e.id_projet,
    e.id_element
  ORDER BY nb DESC
  ) tb2 ON (tb1.id_element = tb2.id_element);

---------------

  -- TODO : externaliser dans l'étape finale d'enrichissement
DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip7;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip7 AS
SELECT DISTINCT t1.id_projet,-- PR-OLU : préférer t1.*
  t1.code_offre,
  t1.code_produit,
  t1.ie_affaire,
  t1.no_element,
  t1.mt_capital_rembourse_client,
  t1.mt_commission_apport_ht,
  t1.mt_commission_risque_ht,
  t1.mt_comserv_convention_ht,
  t1.mt_encours_cumule_moyen,
  t1.mt_financement_ht,
  t1.mt_financement_calef_ht,
  t1.mt_frais_dossier_client,
  t1.mt_frais_dossier_crca,
  t1.mt_interets_client,
  t1.mt_marge_commerciale,
  t1.mt_marge_commerciale_bonifiee,
  t1.mt_marge_frontale,
  t1.mt_marge_frontale_bonifiee,
  t1.mt_pnb_ht,
  t1.mt_premier_loyer,
  t1.mt_valeur_rachat,
  t1.nb_mois,
  t1.nb_echeances,
  t1.nb_echeances_par_an,
  t1.tx_commission_risque,
  t1.tx_comserv_convention,
  t1.tx_marge_financiere,
  t1.tx_nominal_client,
  t1.tx_client,
  t1.tx_premier_loyer_base_locative,
  t1.tx_refi_crca,
  t1.es_crca_region,
  t1.bien_designation,
  t1.bien_nature_code,
  t1.bien_nature,
  t1.bien_categorie,
  t1.assurance_bdm_eligibilite,
  t1.assurance_pfi_eligibilite,
  t1.assurance_adi_eligibilite,
  t2.assurance_bdm_souscription,
  t2.assurance_pfi_souscription,
  t2.assurance_adi_souscription
FROM ${BDD_LEASING_DATA_TMP}.projet_ekip6 t1
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_flag_souscription_equip_2 t2
ON (t1.id_projet = t2.id_projet);

------table intermediaire permettant d'ajouter deux colonnes reseau_apport_code et reseau_apport dans la table projet(sprint8)

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_code_reseau;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_code_reseau AS
SELECT t3.id_affaire,
  t3.reseau_apport_code,
  t4.id_projet,
  t4.id_element,
  t3.libelle,
  t3.Calcul AS reseau_apport
FROM
  (SELECT DISTINCT t1.CODE_RESEAU AS reseau_apport_code,
    t1.id_affaire                 AS id_affaire,
    t2.LIBELLE                    AS libelle,
    (
    CASE
      WHEN t1.code_reseau IN ('','APCL','APCR','APHB','AUBQ','CCF','CCSO','CDBB','CDUP','CHAI', 'CHRV','CMAR','BFCA','CPEL','CPIC','CSAV','CSMC','CUBP')
      THEN 'AUTRES BANQUES'
      WHEN t1.code_reseau IN ('AUCA')
      THEN 'CREDIT AGRICOLE'
      WHEN t1.code_reseau IN ('BAP','BDAF','BE','DCAF','DSI','FIMO')
      THEN 'LCL'
      WHEN t1.code_reseau IN ('CALY')
      THEN 'CACIB'
      WHEN t1.code_reseau IN ('CANO','COUR','FOUR','RICO')
      THEN 'PARTENAIRE'
      WHEN t1.code_reseau IN ('CBF','DR','NEAN')
      THEN 'DIRECT'
      WHEN t1.code_reseau >= 'C700'
      AND t1.code_reseau  <= 'C990'
      THEN 'CREDIT AGRICOLE'
      ELSE t1.code_reseau
    END) AS Calcul
  FROM ${BDD_COMMUN}.ekip_affaire t1
  INNER JOIN ${BDD_COMMUN}.ekip_lib_acodifs t2
  ON (t1.code_reseau = t2.code)
  WHERE t2.typ_code  = 'RESE'
  ) AS t3
INNER JOIN ${BDD_LEASING_DATA_TMP}.element_simul t4
ON (t3.id_affaire = t4.id_affaire);

-----

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_ekip;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_ekip AS
SELECT DISTINCT t1.id_projet,
  t1.ie_affaire as 207_ekip_ie_affaire,
  t1.no_element as 207_ekip_no_element,
  t1.code_offre,
  t1.code_produit,
  t1.mt_capital_rembourse_client,
  t1.mt_commission_apport_ht,
  t1.mt_commission_risque_ht,
  t1.mt_comserv_convention_ht,
  t1.mt_encours_cumule_moyen,
  t1.mt_financement_ht,
  t1.mt_financement_calef_ht,
  t1.mt_frais_dossier_client,
  t1.mt_frais_dossier_crca,
  t1.mt_interets_client,
  t1.mt_marge_commerciale,
  t1.mt_marge_commerciale_bonifiee,
  t1.mt_marge_frontale,
  t1.mt_marge_frontale_bonifiee,
  t1.mt_pnb_ht,
  t1.mt_premier_loyer,
  t1.mt_valeur_rachat,
  t1.nb_mois,
  t1.nb_echeances,
  t1.nb_echeances_par_an,
  t1.tx_commission_risque,
  t1.tx_comserv_convention,
  t1.tx_marge_financiere,
  t1.tx_nominal_client,
  t1.tx_client,
  t1.tx_premier_loyer_base_locative,
  t1.tx_refi_crca,
  t1.es_crca_region,
  t1.bien_designation,
  t1.bien_nature_code,
  t1.bien_nature,
  t1.bien_categorie,
  t1.assurance_bdm_eligibilite,
  t1.assurance_pfi_eligibilite,
  t1.assurance_adi_eligibilite,
  t1.assurance_bdm_souscription,
  t1.assurance_pfi_souscription,
  t1.assurance_adi_souscription,
  t2.reseau_apport_code,
  t2.reseau_apport
FROM ${BDD_LEASING_DATA_TMP}.projet_ekip7 t1
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_code_reseau t2
ON (t1.id_projet = t2.id_projet);

-- Table projet intermediaire liant tiers à projet

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_tiers_ekip;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_tiers_ekip AS
SELECT a.id_projet ,
  t.id_tiers  AS tiers_apporteur_principal ,
  t1.id_tiers AS tiers_apporteur_commissionne ,
  t2.id_tiers AS tiers_agence_CAL ,
  t3.id_tiers AS tiers_commercial_CAL
FROM ${BDD_LEASING_DATA_TMP}.projet_de a
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_tmp t
ON a.n_apporteur_principal=t.ie_tiers
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_tmp t1
ON a.n_apporteur_commissionne=t1.ie_tiers
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_tmp t2
ON a.n_ext_pmp_agence=t2.ie_tiers
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.tiers_tmp t3
ON a.n_ext_pmp_commercial=t3.ie_tiers;



--table prenant les tiers venant de la base nfc à partir de la table tiers_tmp_nfc

DROP TABLE ${BDD_LEASING_DATA_TMP}.projet_tiers_nfc;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_tiers_nfc AS
SELECT t2.id_projet,
  t1.tiers_conseiller,
  t1.tiers_agence,
  t1.TIERS_CRCA
FROM ${BDD_LEASING_DATA_TMP}.projet_tiers_tmp_nfc t1
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_origine_nfc t2
ON (t2.id_projet_origine=t1.short_id)
AND t2.id_application   ='659-LEASENET';

-- Table projet temporaire ajout ekip

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip AS
SELECT
  CASE
    WHEN a.id_projet IS NULL
    THEN b.id_projet
    ELSE a.id_projet
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
  a.es_crca_conseiller,
  a.es_crca_agence,
  a.es_reseau,
  a.bareme,
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
  b.207_ekip_ie_affaire,
  b.207_ekip_no_element,
  b.code_offre,
  b.code_produit,
  b.mt_capital_rembourse_client,
  b.mt_commission_apport_ht,
  b.mt_commission_risque_ht,
  b.mt_comserv_convention_ht,
  b.mt_encours_cumule_moyen,
  b.mt_financement_ht,
  b.mt_financement_calef_ht,
  b.mt_frais_dossier_client,
  b.mt_frais_dossier_crca,
  b.mt_interets_client,
  b.mt_marge_commerciale,
  b.mt_marge_commerciale_bonifiee,
  b.mt_marge_frontale,
  b.mt_marge_frontale_bonifiee,
  b.mt_pnb_ht,
  b.mt_premier_loyer,
  b.mt_valeur_rachat,
  b.nb_mois,
  b.nb_echeances,
  b.nb_echeances_par_an,
  b.tx_commission_risque,
  b.tx_comserv_convention,
  b.tx_marge_financiere,
  b.tx_nominal_client,
  b.tx_client,
  b.tx_premier_loyer_base_locative,
  b.tx_refi_crca,
  b.es_crca_region,
  b.bien_designation,
  b.bien_nature_code,
  b.bien_nature,
  b.bien_categorie,
  b.assurance_bdm_eligibilite,
  b.assurance_pfi_eligibilite,
  b.assurance_adi_eligibilite,
  b.assurance_bdm_souscription,
  b.assurance_pfi_souscription,
  b.assurance_adi_souscription,
  b.reseau_apport_code,
  b.reseau_apport,
-- Ajout des colonnes de la table tiers
  c.tiers_apporteur_principal,
  c.tiers_apporteur_commissionne,
  c.tiers_agence_CAL,
  c.tiers_commercial_CAL,
  d.tiers_conseiller,
  d.tiers_agence,
  d.tiers_crca
FROM ${BDD_LEASING_DATA_TMP}.projet_de_nfc a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_ekip b
ON (a.id_projet = b.id_projet)
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_tiers_ekip c
ON (a.id_projet = c.id_projet)
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_tiers_nfc d
ON (a.id_projet = d.id_projet);


-- table projet temporaire ajout simulbail

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip_simulbail;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip_simulbail AS
SELECT
  CASE
    WHEN a.id_projet IS NULL
    THEN b.id_projet
    ELSE a.id_projet
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
  a.es_crca_conseiller,
  a.es_crca_agence,
  a.es_reseau,
  a.bareme,
  a.207_ekip_ie_affaire,
  a.207_ekip_no_element,
  a.code_offre,
  a.code_produit,
  a.mt_capital_rembourse_client,
  a.mt_commission_apport_ht,
  a.mt_commission_risque_ht,
  a.mt_comserv_convention_ht,
  a.mt_encours_cumule_moyen,
  a.mt_financement_ht,
  a.mt_financement_calef_ht,
  a.mt_frais_dossier_client,
  a.mt_frais_dossier_crca,
  a.mt_interets_client,
  a.mt_marge_commerciale,
  a.mt_marge_commerciale_bonifiee,
  a.mt_marge_frontale,
  a.mt_marge_frontale_bonifiee,
  a.mt_pnb_ht,
  a.mt_premier_loyer,
  a.mt_valeur_rachat,
  a.nb_mois,
  a.nb_echeances,
  a.nb_echeances_par_an,
  a.tx_commission_risque,
  a.tx_comserv_convention,
  a.tx_marge_financiere,
  a.tx_nominal_client,
  a.tx_client,
  a.tx_premier_loyer_base_locative,
  a.tx_refi_crca,
  a.es_crca_region,
  a.bien_designation,
  a.bien_nature_code,
  a.bien_nature,
  a.bien_categorie,
  a.assurance_bdm_eligibilite,
  a.assurance_pfi_eligibilite,
  a.assurance_adi_eligibilite,
  a.assurance_bdm_souscription,
  a.assurance_pfi_souscription,
  a.assurance_adi_souscription,
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
  a.reseau_apport_code,
  a.reseau_apport,
  a.tiers_apporteur_principal,
  a.tiers_apporteur_commissionne,
  a.tiers_agence_CAL,
  a.tiers_commercial_CAL,
  a.tiers_conseiller,
  a.tiers_agence,
  a.tiers_crca,
  	CASE
		 WHEN a.tx_comserv_convention=0 THEN ROUND((( a.mt_comserv_convention_ht/12)*a.nb_echeances),6)
		 ELSE   ROUND((a.tx_comserv_convention*a.mt_encours_cumule_moyen),6)
	END as mt_comserv_calef_ht,
	case
		 when  a.mt_comserv_convention_ht =0 THEN a.tx_comserv_convention
		 ELSE ROUND(((( a.mt_comserv_convention_ht/12)*a.nb_echeances)*(a.tx_nominal_client - a.tx_refi_crca - a.tx_commission_risque)) /(a.mt_commission_apport_ht+(( a.mt_comserv_convention_ht/12)*a.nb_echeances)),6)
	END as tx_comserv_calef
FROM ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip a
FULL OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_simulbail_only b
ON (a.id_projet = b.id_projet);


-- Table comprenant les délais entre création, edition, signature et mise en loyer

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_air_evenement;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_air_evenement AS
SELECT * ,
  CASE
    WHEN DATEDIFF(dt_edition_nn, dt_creation_nn) >= 0
    THEN CAST((unix_timestamp(dt_edition_nn) - unix_timestamp( dt_creation_nn))/60 /60 /24 as decimal(15,1))
    ELSE 0
  END AS delai_edition ,
  CASE
    WHEN DATEDIFF(dt_signature_nn, dt_edition_nn) >= 0
    THEN CAST( (unix_timestamp(dt_signature_nn) - unix_timestamp( dt_edition_nn))/60 /60 /24 as decimal(15,1))
    ELSE 0
  END AS delai_signature ,
  CASE
    WHEN DATEDIFF(dt_mel_nn, dt_signature_nn) >=0
    THEN CAST( (unix_timestamp(dt_mel_nn) - unix_timestamp( dt_signature_nn))/60 /60 /24 as decimal(15,1))
    ELSE 0
  END AS delai_mel
FROM
  (SELECT * ,
    CASE
      WHEN dt_creation IS NOT NULL
      THEN dt_creation
      WHEN dt_edition IS NOT NULL
      THEN dt_edition
      WHEN dt_signature IS NOT NULL
      THEN dt_signature
      WHEN dt_mel IS NOT NULL
      THEN dt_mel
      ELSE NULL
    END AS dt_creation_nn ,
    CASE
      WHEN dt_edition IS NOT NULL
      THEN dt_edition
      WHEN dt_signature IS NOT NULL
      THEN dt_signature
      WHEN dt_mel IS NOT NULL
      THEN dt_mel
      ELSE NULL
    END AS dt_edition_nn ,
    CASE
      WHEN dt_signature IS NOT NULL
      THEN dt_signature
      WHEN dt_mel IS NOT NULL
      THEN dt_mel
      ELSE NULL
    END    AS dt_signature_nn ,
    dt_mel AS dt_mel_nn
  FROM
    (SELECT p.id_projet,
      MIN(e_creation.date_evenement ) dt_creation,
      MIN(e_edition.date_evenement ) dt_edition,
      MIN(e_signature.date_evenement ) dt_signature,
      MAX(e_mel.date_evenement ) dt_mel
    FROM ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip_simulbail p
    LEFT OUTER JOIN ${BDD_LEASING_DATA}.projet_evenement e_creation
    ON p.id_projet          =e_creation.id_projet
    AND e_creation.evenement='CREE'--'Création'
    LEFT OUTER JOIN ${BDD_LEASING_DATA}.projet_evenement e_edition
    ON p.id_projet         =e_edition.id_projet
    AND e_edition.evenement='Edition'
    LEFT OUTER JOIN ${BDD_LEASING_DATA}.projet_evenement e_signature
    ON p.id_projet           =e_signature.id_projet
    AND e_signature.evenement='Signature'
    LEFT OUTER JOIN ${BDD_LEASING_DATA}.projet_evenement e_mel
    ON p.id_projet     =e_mel.id_projet
    AND e_mel.evenement='Mise en Loyer'
    GROUP BY p.id_projet
    ) AS source
  ) AS source ;

  -------  Etape de transformation avec PDLR ---
-- Raptriement de tous les dossiers PDLR avec leur id technique

DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.element_tmp_pdlr;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.element_tmp_pdlr AS
SELECT b.id_projet,
  a.id_element,
  a.id_affaire
FROM ${BDD_COMMUN}.pdlr_element a
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_origine_tmp b
ON (a.id_element = b.id_projet_origine
AND b.id_application="040A")
;

 -- Table tempraire contenant les projets de differentes application avec les delais des fransformation
 DROP TABLE IF EXISTS ${BDD_LEASING_DATA_TMP}.projet_tmp;
CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_tmp AS
SELECT
  a.*,
  d.delai_edition ,
  d.delai_signature ,
  d.delai_mel
FROM ${BDD_LEASING_DATA_TMP}.projet_de_nfc_ekip_simulbail a
LEFT OUTER JOIN ${BDD_LEASING_DATA_TMP}.projet_air_evenement d
ON (a.id_projet = d.id_projet);