#============== Funktioner och en wrapper-funktion för att köra DjikstraNearCost på två tabeller med punkter och ett nätverk ============
# Funktionen avstandsanalys_tva_punkttabeller_ett_natverk() är den funktion som anropar de andra funktionerna,
# Dessa funktioner kan köras separat men bör isf följa samma ordning som i wrapper-funktionen

# 0. Funktion för att koppla upp mot databasen. Kan användas med defaultvärden enligt nedan eller egna parametrar.
# Används av andra funktioner som default om inget eget objekt med databasuppkoppling har skickats till dessa funktioner
# OBS! Ändra default för db_name till "geodata" sen
uppkoppling_db <- function(
    service_name = "rd_geodata",
    db_host = "WFALMITVS526.ltdalarna.se",
    db_port = 5432,
    db_name = "praktik",                    # Ändra till "geodata" sen
    db_options = "-c search_path=public"
) {
  
  tryCatch({
    # Etablera anslutningen
    con <- dbConnect(          
      RPostgres::Postgres(),
      bigint = "integer",  
      user = key_list(service = service_name)$username,
      password = key_get(service_name, key_list(service = service_name)$username),
      host = db_host,
      port = db_port,
      dbname = db_name,
      options=db_options)
    
    # Returnerar anslutningen om den lyckas
    return(con)
  }, error = function(e) {
    # Skriver ut felmeddelandet och returnerar NULL
    print(paste("Ett fel inträffade vid anslutning till databasen:", e$message))
    return(NULL)
  })
  
}


# 1. Funktion för att utöka nätverk med nya noder
hitta_narmaste_punkt_pa_natverk <- function(
    con = "default",         # Om ingen uppkoppling skickas in används default-uppkopplingen i uppkoppling_db()
    schema_fran,             # Schemat som innehåller tabellen med punkter
    tabell_fran,             # Tabell med punkter som skall användas för att utöka ett nätverk/graf
    geometri_fran = "geometry",# Kolumnen med geometrin i tabell_fran
    id_fran = "id",          # Kolumnen som innehåller id för tabellen med punkter
    schema_graf,             # Schemat som innehåller tabellen med nätverket/grafen
    tabell_graf,             # Tabell med ett nätverk som skall utökas med nya noder från tabell_fran
    geometri_graf = "geometry",# Kolumnen med geometrin i tabell_graf
    id_graf = "id",          # Kolumnen som innehåller id för tabellen med grafen
    tolerans_avstand = 3     # Parameter för toleransen i meter för att skapa nya adresspunkter, används för att 1. Inte skapa ny nod om en existerar inom X m från ny nod och 2. för att skapa kluster av nya noder inom X m från varandra och välja en representant
) {
  # Starta tidstagning
  starttid <- Sys.time()
  
  # Sätt en flagga som kontrollerar ifall defaultuppkoppling är använd, isf skall den sättas till true så att vi kan stänga den sen
  default_flagga = FALSE
  # Kontrollera om anslutningen är en teckensträng och skapa uppkoppling om så är fallet
  if(is.character(con) && con == "default") {
    con <- uppkoppling_db()  # Anropa funktionen för att koppla upp mot db med defaultvärden
    default_flagga = TRUE
  }
  
  
  dbBegin(con)  # Starta en databastransaktion
  
  tryCatch({
    
    # Kolla om kolumnen original_id finns i tabell_graf annars skapa den med NULL-värden
    # Behövs för att behålla id till korrekt segment i originalgrafen för att plocka ut annan info
    dbExecute(con, glue("DO $$
                          BEGIN
                              -- Kontrollera om kolumnen original_id finns i den aktuella grafen
                              IF NOT EXISTS (
                                  SELECT 1 
                                  FROM information_schema.columns 
                                  WHERE 
                                      table_schema = '{schema_graf}' AND 
                                      table_name = '{tabell_graf}' AND 
                                      column_name = 'original_id'
                              ) THEN
                                  -- Om den inte finns, skapa den
                                  ALTER TABLE {schema_graf}.{tabell_graf} ADD COLUMN original_id INT;
                              END IF;
                          END$$;"))
    
    # 0. Gör om ZM Linestrings till 2D
    # dbExecute(con, glue("ALTER TABLE {schema}.{tabell_graf} ALTER COLUMN {geom_kolumn} TYPE geometry(Geometry, 3006) USING ST_Force2D({geom_kolumn});"))
    # 1 Skapa tabell med alla nya punkter som skall användas för att dela upp vägsegmenten - dvs närmaste punkten på en linje för varje objekt i tabell_fran
    # Använder tolerans_avstand för att avgöra om den skall lägga in en ny punkt eller "använda" en existerande
    sql_query <- glue("CREATE TEMP TABLE narmaste_punkt AS
                      WITH closest_points AS (
                        SELECT
                          f.{id_fran} AS adress_id,
                          g.{id_graf} AS graf_id,
                          ST_ClosestPoint(g.{geometri_graf}, f.{geometri_fran}) AS punkt_pa_natverk,
                          ST_DWithin(ST_ClosestPoint(g.{geometri_graf}, f.{geometri_fran}), ST_StartPoint(g.{geometri_graf}), {tolerans_avstand}) AS is_near_start_point,
                          ST_DWithin(ST_ClosestPoint(g.{geometri_graf}, f.{geometri_fran}), ST_EndPoint(g.{geometri_graf}), {tolerans_avstand}) AS is_near_end_point
                        FROM
                          {schema_fran}.{tabell_fran} f,
                          LATERAL (
                            SELECT g.{id_graf}, g.{geometri_graf}
                            FROM {schema_graf}.{tabell_graf}  g
                            ORDER BY f.{geometri_fran} <-> g.{geometri_graf}
                            LIMIT 1
                          ) AS g
                      )
                      SELECT adress_id, graf_id, punkt_pa_natverk
                      FROM closest_points
                      WHERE NOT is_near_start_point AND NOT is_near_end_point;
                      ")
    
    dbExecute(con, sql_query)
    print("Nya punkter hittade på nätverket. Skapar kluster och indexerar position på linjerna...")
    # 2 Skapa kluster av alla nya punkter som ligger inom tolerans_avstånd meter ifrån varandra och välj en av dessa som representant
    # OBS! Kluster beräknas på att de ligger på samma linje för att undvika att den klustrar med en punkt på en annan linje.
    # Räkna samtidigt ut dess relativa position på linjen (0-1) som används i nästa steg för att skapa nya linjer
    #- tester har gett att med en tolerans på 1 m -> 5% färre nya punkter, 2 m -> 10% färre punkter
    # Borde kunna gå att räkna ut centroiden i clustret och sedan hitta den närmaste punkten på vägnätverket om man vill ha en bättre "medelpunkt"
    sql_cluster <- glue("CREATE TEMP TABLE klusterpunkt AS
                      WITH clusters AS ( -- Skapar kluster av punkter inom tolerans_avstånd meter från varandra - minsta antalet 1 så att alla punkter kommer med
                        SELECT
                          ST_ClusterDBSCAN(punkt_pa_natverk, eps := {tolerans_avstand}, minpoints := 1) OVER(PARTITION BY graf_id) AS cid,
                          punkt_pa_natverk,
                          graf_id
                        FROM
                          narmaste_punkt
                      ), cluster_centroids AS ( -- Hittar centroiden i alla kluster
                        SELECT
                          cid,
                          graf_id,
                          ST_Centroid(ST_Collect(punkt_pa_natverk)) AS centroid
                        FROM clusters
                        GROUP BY cid, graf_id
                      ), representant_punkter AS ( -- Välj ut den punkt i klustret som är närmast centroiden som respresentant. Sätt också dess relativa position på 'gamla' linjen (line_location) för att kunna i nästa steg skapa en substring
                        SELECT DISTINCT ON (c.cid, c.graf_id)
                          c.cid,
                          c.graf_id,
                          n.original_id,
                          c.punkt_pa_natverk AS representant_punkt,
                          ST_LineLocatePoint(n.{geometri_graf}, c.punkt_pa_natverk) AS line_location
                        FROM clusters c
                        JOIN {schema_graf}.{tabell_graf} n ON c.graf_id = n.id
                        JOIN cluster_centroids cc ON c.cid = cc.cid AND c.graf_id = cc.graf_id
                        ORDER BY c.cid, c.graf_id, ST_Distance(c.punkt_pa_natverk, cc.centroid)
                      )
                      SELECT
                        cid,
                        graf_id,
                        original_id,
                        representant_punkt,
                        line_location
                      FROM representant_punkter
                      ORDER BY graf_id, line_location; -- Se till att sortera alla nya punkter med dess relativa position inför nästa steg
                  ")
    
    dbExecute(con, sql_cluster)
    print("Kluster beräknade och position på nätverket indexerad. Skapar nytt nätverk...")
    # 3 Splitta linjer för nya segment utifrån de närmaste punkterna -> det nya nätverket. 
    # ID till original-nätverket sparas i original_id för att kunna användas vid join för att hämta ex.vis hastighet
    tabell_ny_natverk <- glue("{tabell_graf}_{tabell_fran}")
    
    # Kontrollera ifall den nya tabellen redan finns, isf DROPPA den
    
    res <- dbGetQuery(con, glue("SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_graf}' AND table_name = '{tabell_ny_natverk}';"))
    
    if (nrow(res) == 1) {
      dbExecute(con, glue("DROP TABLE {schema_graf}.{tabell_ny_natverk};"))
      print(glue("Tabellen {tabell_ny_natverk} fanns redan och har tagits bort."))
    }
    
    # 4. Skapa den nya tabellen som innehåller den nya grafen som skapats utifrån de närmaste punkterna i tabell_fran
    sql_create_table <- glue("CREATE TABLE {schema_graf}.{tabell_ny_natverk} AS 
                            WITH locus AS (
                              SELECT {id_graf} AS gid, original_id, 0 as l
                              FROM {schema_graf}.{tabell_graf}
                              UNION ALL
                              SELECT {id_graf} AS gid, original_id, 1 as l
                              FROM {schema_graf}.{tabell_graf}
                              UNION ALL
                              SELECT graf_id AS gid, original_id, line_location as l
                              FROM klusterpunkt
                            ),
                            loc_with_idx AS (
                              SELECT gid, original_id, l, rank() OVER (PARTITION BY gid ORDER BY l) AS idx
                              FROM locus
                            )
                            SELECT
                              ROW_NUMBER() OVER (ORDER BY loc1.gid, loc1.idx) AS {id_graf}, -- Lägger till nytt 'id' som räknare
                              loc1.gid,
                              COALESCE(loc1.original_id, loc1.gid) AS original_id,
                              ST_LineSubstring(
                                n.{geometri_graf}, 
                                loc1.l, 
                                loc2.l
                              ) AS {geometri_graf}
                            FROM 
                              loc_with_idx loc1 JOIN loc_with_idx loc2 USING (gid)
                            JOIN 
                              {schema_graf}.{tabell_graf} n ON loc1.gid = n.{id_graf}
                            WHERE loc2.idx = loc1.idx+1;
                        ")
    
    dbExecute(con, sql_create_table)
    
    # Lägg till index på geometrin i det nya nätverket
    dbExecute (con, glue("CREATE INDEX {tabell_ny_natverk}_geometry_idx ON {schema_graf}.{tabell_ny_natverk} USING GIST({geometri_graf});"))
    print(glue("Nytt nätverk {tabell_ny_natverk} skapat."))
    
    
    dbCommit(con)  # Bekräfta transaktionen
  }, error = function(e) {
    dbRollback(con)  # Ångra transaktionen vid fel
    cat("Det uppstod ett fel under processen: ", e$message)
  })
  # Droppa temp-tabellerna om de ff finns kvar
  dbExecute(con, glue("DROP TABLE IF EXISTS narmaste_punkt;"))
  dbExecute(con, glue("DROP TABLE IF EXISTS klusterpunkt;"))
  # Koppla ner om defaultuppkopplingen har använts
  if(default_flagga){
    dbDisconnect(con)
  }
  
  
  # Beräkna och skriv ut tidsåtgång
  sluttid <- Sys.time()
  tidstagning <- sluttid - starttid
  message(sprintf("Processen tog %s att köra", tidstagning))
}

# 2. FUnktion för att skapa graf av tabell med linjer, ex.vis nvdb
postgis_tabell_till_pgrgraf <- function(
    con = "default",              # Möjlighet att skicka in en redan etablerad uppkoppling mot db, annars används defaultuppkopplingen i uppkoppling_db()
    schema_graf = "nvdb",         # Vilket schema ligger tabellen i. Default: nvdb
    tabell_graf = "nvdb20buff30", # Tabellen som ska förberedas för pgrouting. Default: nvdb20buff30
    id_graf = "id",               # Kolumnen med id i graf-tabellen
    geom_kolumn = "geometry",         # Namn på kolumnen som innehåller geometrin
    tolerans = 0.001           # Toleransvärdet för hur nära segment måste vara för att ansluta. 
){
  
  # Sätt en flagga som kontrollerar ifall defaultuppkoppling är använd, isf skall den sättas till true så att vi kan stänga den sen
  default_flagga = FALSE
  # Kontrollera om anslutningen är en teckensträng och skapa uppkoppling om så är fallet
  if(is.character(con) && con == "default") {
    con <- uppkoppling_db()  # Anropa funktionen för att koppla upp mot db med defaultvärden
    default_flagga = TRUE
  }
  
  # Skapa topologi till pgrouting, från G:\skript\gis\postgis\pgrouting test.R 
  # Använder BEGIN och COMMIT tillsammans med tryCatch för att säkerställa att allt görs eller inget
  tryCatch({
    dbBegin(con)
    # Kolla om topologi-tabellen redan finns, isf ta bort den
    res <- dbGetQuery(con, glue("SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_graf}' AND table_name = '{tabell_graf}_vertices_pgr';"))
    
    if (nrow(res) == 1) {
      dbExecute(con, glue("DROP TABLE {schema_graf}.{tabell_graf}_vertices_pgr;"))
      print(glue("Tabellen {tabell_graf}_vertices_pgr fanns redan och har tagits bort."))
    }
    
    # Ta bort zm-värden från geometrin - krävs för pgrouting
    dbExecute(con, glue("ALTER TABLE {schema_graf}.{tabell_graf} ALTER COLUMN {geom_kolumn} TYPE geometry(Geometry, 3006) USING ST_Force2D({geom_kolumn});"))
    # Skapa index på geometrin
    dbExecute(con, glue("CREATE INDEX {tabell_graf}_{geom_kolumn} ON {schema_graf}.{tabell_graf} USING GIST ({geom_kolumn});"))
    
    # se till att id är av typen integer
    dbExecute(con, glue("ALTER TABLE {schema_graf}.{tabell_graf} ALTER COLUMN {id_graf} TYPE integer USING id::integer;"))
    # lägg till kolumnen "source"
    dbExecute(con, glue("ALTER TABLE {schema_graf}.{tabell_graf} ADD COLUMN IF NOT EXISTS source integer;"))
    # lägg till kolumnen "target"
    dbExecute(con, glue("ALTER TABLE {schema_graf}.{tabell_graf} ADD COLUMN IF NOT EXISTS target integer;"))
    # skapa topologi i pgrouting
    dbExecute(con, glue("SELECT pgr_createTopology('{schema_graf}.{tabell_graf}', '{tolerans}', '{geom_kolumn}', '{id_graf}');"))
    
    # analysera den nyligen skapade topologin
    dbExecute(con, glue("SELECT pgr_analyzeGraph('{schema_graf}.{tabell_graf}', {tolerans}, the_geom := '{geom_kolumn}', id := '{id_graf}');"))
    
    # Skapa kolumn för kostnad i meter
    dbExecute(con, glue("ALTER TABLE {schema_graf}.{tabell_graf} ADD COLUMN IF NOT EXISTS kostnad_meter double precision;"))
    # Sätt kostnad i meter för varje segment
    dbExecute(con, glue("UPDATE {schema_graf}.{tabell_graf} SET kostnad_meter = ST_Length({geom_kolumn});"))
    
    
    #Om allt gått bra, committa
    dbCommit(con)
    
    #Om något fel skett i blocket, kör en rollback
  }, error = function(e) {
    dbRollback(con)
    stop("Transaktionen misslyckades: ", e$message)
  }, finally = {
    
    # Koppla ner om defaultuppkopplingen har använts
    if(default_flagga){
      dbDisconnect(con)
      print("Uppkopplingen avslutad!")
    }
    
  }
  )
}

# 3. Funktion för att koppla tabell med punkter till pgRoutinggraf
koppla_punkttabell_till_pgr_graf <- 
  function(con = "default", # Möjlighet att skicka in en redan etablerad uppkoppling mot db, annars används defaultuppkopplingen i uppkoppling_db()
           schema_punkter,
           tabell_punkter,   
           geom_kolumn = "geometry",
           schema_natverk = "nvdb",
           tabell_natverk = "nvdb20buff30"
  ){
    
    # Sätt en flagga som kontrollerar ifall defaultuppkoppling är använd, isf skall den sättas till true så att vi kan stänga den sen
    default_flagga = FALSE
    # Kontrollera om anslutningen är en teckensträng och skapa uppkoppling om så är fallet
    if(is.character(con) && con == "default") {
      con <- uppkoppling_db()  # Anropa funktionen för att koppla upp mot db med defaultvärden
      default_flagga = TRUE
    }
    
    # gör en spatial join för mittpunkten i rutorna till nvdb ====================
    tryCatch({
      dbBegin(con)
      # vi börjar med att skapa en ny kolumn i den nya tabellen
      dbExecute(con, glue("ALTER TABLE {schema_punkter}.{tabell_punkter} ADD COLUMN IF NOT EXISTS toponode_id bigint;"))
      # Töm kolumnen om där redan finns värden
      dbExecute(con, glue("UPDATE {schema_punkter}.{tabell_punkter} SET toponode_id = NULL;"))
      # därefter gör vi en spatial join från mittpunkten till noderna i nvdb
      dbExecute(con, glue("UPDATE {schema_punkter}.{tabell_punkter} 
                          SET toponode_id = (
                            SELECT {schema_natverk}.{tabell_natverk}_vertices_pgr.id 
                            FROM {schema_natverk}.{tabell_natverk}_vertices_pgr 
                            ORDER BY {schema_punkter}.{tabell_punkter}.{geom_kolumn} <-> 
                            {schema_natverk}.{tabell_natverk}_vertices_pgr.the_geom ASC NULLS LAST 
                            LIMIT 1);"))
      # Om allt gått bra, committa
      dbCommit(con)
    }, error = function(e) {
      # Om något gått fel under processen, återställ databasen till innan denna funktion
      dbRollback(con)
      print("Transaktionen misslyckades: ", e$message)
    }) 
    
    # Koppla ner om defaultuppkopplingen har använts
    if(default_flagga){
      dbDisconnect(con)
      print("Uppkopplingen avslutad.")
    }
    
  } # slut funktion

# 4. Funktion för att köra DjistraNearCost på två st tabeller med punkter och ett nätverk
berakna_pgr_djikstranearcost_batch <- function(
    con = "default",
    schema_fran,
    tabell_fran,
    schema_till,
    tabell_till,
    schema_graf,
    tabell_graf,
    id_graf,
    batch_storlek = 1000  # Ny parameter för batch-storlek
){
  # Starta tidstagning
  starttid <- Sys.time()
  
  # Sätt en flagga som kontrollerar ifall defaultuppkoppling är använd, isf skall den sättas till true så att vi kan stänga den sen
  default_flagga = FALSE
  # Kontrollera om anslutningen är en teckensträng och skapa uppkoppling om så är fallet
  if(is.character(con) && con == "default") {
    con <- uppkoppling_db()  # Anropa funktionen för att koppla upp mot db med defaultvärden
    default_flagga = TRUE
  }
  
  # Skapa en ny tabell med alla unika toponode_id i tabell_fran om den inte redan finns. Om den finns så töm tabellen först
  # 1. SKapa den nya tabellens namn, en kombination av de två tabellerna fran och till
  tabell_ny <- glue("{tabell_fran}_{tabell_till}")
  
  # 2. Kolla om den redan finns, isf töm den
  res <- dbGetQuery(con, glue("SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_fran}' AND table_name = '{tabell_ny}';"))
  if (nrow(res) == 0) {
    # Skapa den nya tabellen med samma kolumnnamn som djikstra_result
    dbExecute(con, glue("CREATE table IF NOT EXISTS {schema_fran}.{tabell_ny}(
	                          start_vid int,
	                          end_vid int,
	                          agg_cost double precision
                        );"))
    
    print(glue("Tabellen {tabell_ny} har skapats."))
  } else {
    dbExecute(con, glue("TRUNCATE TABLE {schema_fran}.{tabell_ny};"))
    print(glue("Tabellen {tabell_ny} fanns redan och har tömts på värden."))
  }
  # Skapa temporära tabeller för toponode_idn från tabell_fran och tabell_till, skall användas i djikstranearcost
  temp_fran <- glue("temp_{tabell_fran}")
  dbExecute(con, glue("CREATE TEMP TABLE {temp_fran} AS SELECT DISTINCT toponode_id FROM {schema_fran}.{tabell_fran};"))
  temp_till <- glue("temp_{tabell_till}")
  dbExecute(con, glue("CREATE TEMP TABLE {temp_till} AS SELECT DISTINCT toponode_id FROM {schema_till}.{tabell_till};"))
  
  # Skapa index till temptabellerna, behöver ej droppas efter de försvinner automagiskt när tabellen droppas
  dbExecute(con, glue("CREATE INDEX temp_fran_toponode_idx ON {temp_fran}(toponode_id);"))
  dbExecute(con, glue("CREATE INDEX temp_till_toponode_idx ON {temp_till}(toponode_id)"))
  
  # Skapa förutsättningar för batchkörning
  # Räkna antalet rader i tabell_ny
  antal_noder <- dbGetQuery(con, glue("SELECT COUNT(*) as antal FROM {temp_fran}"))$antal
  
  # Beräkna antalet batcher
  antal_batcher <- ceiling(antal_noder / batch_storlek)
  
  print(glue("Letar efter kortaste vägen från {antal_noder} noder i {antal_batcher} batcher."))
  ###
  for(i in 1:antal_batcher){
    
    tryCatch({
      offset <- (i - 1) * batch_storlek
      
      # Kör DJikstraNearCost med start- och slutvid från temptabellerna med unika noder och stoppa in i tabell_ny
      sql_query <-  glue("INSERT INTO {schema_fran}.{tabell_ny} (start_vid, end_vid, agg_cost)
                            SELECT
                                start_vid,
                                end_vid,
                                agg_cost
                            FROM pgr_dijkstraNearCost(
                                'SELECT {id_graf}, source, target, kostnad_meter AS cost FROM {schema_graf}.{tabell_graf}',
                                ARRAY(SELECT toponode_id FROM {temp_fran} LIMIT {batch_storlek} OFFSET {offset})::INT[],
                                ARRAY(SELECT toponode_id FROM {temp_till})::INT[],
                                directed => false,
                                global => false
                            ) AS djikstra_result;")
      
      dbExecute(con, sql_query)
      print(glue("Batch {i} av {antal_batcher} bearbetad."))
      #Sys.sleep(90)
    }, warning = function(w) {
      message("Varning upptäckt: ", w)
    }, error = function(e) {
      
      print(glue("Fel under bearbetning av batch {i}: ", e$message))
    }
    )
  }
  #####
  
  # Leta noder där start och slutnod är samma och sätt kostnad till 0
  sql_query <- glue("INSERT INTO {schema_fran}.{tabell_ny} (start_vid, end_vid, agg_cost)
                        SELECT t.toponode_id, tt.toponode_id, 0
                        FROM {temp_till} tt
                        INNER JOIN {temp_fran} t ON tt.toponode_id = t.toponode_id
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {schema_fran}.{tabell_ny} ny
                            WHERE ny.start_vid = t.toponode_id
                        );")
  #print (sql_query)
  dbExecute(con, sql_query)
  # Droppa temptabellen
  dbExecute(con, glue("DROP TABLE IF EXISTS {temp_fran};"))
  dbExecute(con, glue("DROP TABLE IF EXISTS {temp_till};"))
  
  # Kolla hur många rader som finns i den nya tabellen, bör stämma med antalet unika noder
  antal_rader <- dbGetQuery(con, glue("SELECT COUNT(*) as antal FROM {schema_fran}.{tabell_ny}"))$antal
  
  print(glue("Det tog {round(difftime(Sys.time(), starttid, units = \"auto\"),1)} minuter att fylla tabellen {schema_fran}.{tabell_ny} med {antal_rader} rader, att jämföra med {antal_noder} unika noder."))
  
  if(antal_rader != antal_noder){
    print("Det finns en differens mellan förväntat och faktiskt resultat vilket indikerar att något blivit fel.")
  }
  
  # Koppla ner om defaultuppkopplingen har använts
  if(default_flagga){
    dbDisconnect(con)
    print("Uppkopplingen avslutad.")
  }
  
  
}

# Wrapper-funktion för att köra djikstraNearCost på två tabeller med punkter och ett nätverk
avstandsanalys_tva_punkttabeller_ett_natverk <- function(
    w_con = "default",     # Om ingen uppkoppling skickas in används default-uppkopplingen i uppkoppling_db()
    schema_fran,           # Schemat som innehåller från-tabellen med punkter
    tabell_fran,           # Tabell med punkter som analysen använder som "från"
    id_fran,               # Kolumnen som innehåller id för från-tabellen
    geometri_fran,         # Kolumnen som innehåller geometrin för från-tabellen
    schema_till,           # Schemat som innehåller till-tabellen med punkter
    tabell_till,           # Tabell med punkter som analysen använder som "till"
    id_till,               # Kolumnen som innehåller id för till-tabellen
    geometri_till,         # Kolumnen som innehåller geometrin för till-tabellen
    schema_graf,           # Schemat som innehåller tabellen med nätverket/grafen
    tabell_graf,           # Tabell med ett nätverk som avståndsanalysen ska köras på
    id_graf,               # Kolumnen som innehåller id för nätverkstabellen
    geometri_graf,         # Kolumnen som innehåller geometrin för nätverkstabellen
    tolerans_avstand = 3   # Toleransen i meter för gränsvärde när den skall skapa nya noder
)
{
  # Hantera uppkoppling, är den default så anropa uppkoppling_db() med defaultvärden
  if(is.character(w_con) && w_con == "default") {
    w_con <- uppkoppling_db()  # Anropa funktionen för att koppla upp mot db med defaultvärden
  }
  
  # Variabler som innehåller nya tabellnamn deriverade från de tabeller som skickas in
  mellan_graf = glue("{tabell_graf}_{tabell_fran}")  # Resultatet av den första körningen av hitta_narmaste_punkt_pa_natverk(), skickas till den andra körningen
  slutlig_graf = glue("{mellan_graf}_{tabell_till}") # # Resultatet av den andra körningen av hitta_narmaste_punkt_pa_natverk()
  
  # Då wrapper-funktionen använder samma parameternamn som underfunktionerna behövs de sparas ner i en lista "args"
  args <- as.list(match.call())[-1]
  
  # 1. SKapa nytt nätverk med från-tabellen 
  hitta_narmaste_punkt_pa_natverk(
    con = w_con,
    schema_fran = args$schema_fran,
    tabell_fran = args$tabell_fran,
    geometri_fran = args$geometri_fran,
    id_fran = args$id_fran,
    schema_graf = args$schema_graf,
    tabell_graf = args$tabell_graf,
    geometri_graf = args$geometri_graf,
    id_graf = args$id_graf,
    tolerans_avstand = args$tolerans_avstand
  )
  
  
  # 2. SKapa nytt nätverk med till-tabellen och utgå ifrån det nya nätverket skapat i steg 1
  hitta_narmaste_punkt_pa_natverk(
    con = w_con,
    schema_fran = args$schema_till,
    tabell_fran = args$tabell_till,
    geometri_fran = args$geometri_till,
    id_fran = args$id_till,
    schema_graf = args$schema_graf,
    tabell_graf = mellan_graf,
    geometri_graf = args$geometri_graf,
    id_graf = args$id_graf,
    tolerans_avstand = args$tolerans_avstand
  )
  
  # 3. Koppla det nya nätverket från steg 2 till en pgrgraf
  postgis_tabell_till_pgrgraf(
    con = w_con,
    geom_kolumn = geometri_graf, 
    schema_graf = args$schema_graf,
    tabell_graf = slutlig_graf,
  )
  
  # 4. Koppla tabell_fran till pgr_grafen
  koppla_punkttabell_till_pgr_graf(
    con = w_con,
    schema_punkter = schema_fran,
    tabell_punkter = tabell_fran,   
    geom_kolumn = geometri_fran,
    schema_natverk = schema_graf,
    tabell_natverk = slutlig_graf
  ) 
  
  # 5. Koppla tabell_till till pgr_grafen
  koppla_punkttabell_till_pgr_graf(
    con = w_con,
    schema_punkter = schema_till,
    tabell_punkter = tabell_till,   
    geom_kolumn = geometri_till,
    schema_natverk = schema_graf,
    tabell_natverk = slutlig_graf
  )
  
  # 6. Kör pgr_djikstranearcost på resultatet av tidigare steg
  berakna_pgr_djikstranearcost_batch(
    con = w_con,
    schema_fran = args$schema_fran,
    tabell_fran = args$tabell_fran,
    schema_till = args$schema_till,
    tabell_till = args$tabell_till,
    schema_graf = args$schema_graf,
    tabell_graf = slutlig_graf,
    id_graf = args$id_graf
  )
  
  # Koppla ner
  dbDisconnect(w_con)
  print("Uppkoppling avslutad.")
}