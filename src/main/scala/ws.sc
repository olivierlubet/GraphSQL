import java.io.File

import graphsql.controler.SQLFileLoader


val q = "CREATE TABLE BDD_LEASING_DATA.projet_conquete  LIKE BDD_LEASING_DATA_TMP.projet_conquete_tmp STORED AS PARQUET blabla"
q.replaceAll("STORED AS PARQUET","")

SQLFileLoader.eraseSomeKeywords(q)

List("STORED AS PARQUET")
  .foldLeft(q)((str,kw) => str.replaceAll(kw,""))