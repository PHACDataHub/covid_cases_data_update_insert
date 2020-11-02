


source("functions.r")


PT = "SK"
DAYS_AGO = 1








find_fn_of_PT <- function(
                         a_pt,
                         a_date = Sys.Date()-DAYS_AGO,
                         a_base_dir = CANADA_CASE_REPORTS){
  #'
  #' get the filename that the new PT data is stored in
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param a_base_dir Directory where PT incoming data for cases is stored 
  
  
  
  
  helper <- get_insert_helper(a_pt)
  
  
  
  date_formated <- format(a_date, helper$date_format)
  str_interp(helper$file_nm_format)
  
  # if (a_pt == "SK"){
  #   return(paste0(format(a_date, "%Y-%m-%d"), " COVID SK.xlsx"))
  # }
  # 
  # if (a_pt == "MB"){
  #   return(paste0("Cases_", format(a_date, "%Y%m%d"), "_MB.xlsx"))  
  # }
}




find_full_fn_of_PT <- function(a_pt, 
                               a_date = Sys.Date()-DAYS_AGO,
                               a_base_dir = CANADA_CASE_REPORTS, 
                               ...){
  #'
  #' get some insert configuration suff for the given PT
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param a_base_dir Directory where PT incoming data for cases is stored 
  file.path(a_base_dir, a_pt, find_fn_of_PT(a_pt = a_pt,
                                            a_date = a_date,
                                             ...))
}



get_insert_helper <-function(a_pt){
  #'
  #' get some insert configuration suff for the given PT
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....

  fn <- "insert_helper.xlsx"
  helper <- readxl::read_xlsx(fn)%>% 
    filter(pt == a_pt) 
  
  if(nrow(helper) != 1) {
    n_row <- nrow(helper)
    warning(str_interp("The PT '${a_pt}' there were ${n_row} rows returned from the file '${fn}'. This is a big problem!"))
    return(NULL)
  }
  
  return(helper %>% as.list())
}


read_pt_file <- function(a_pt, 
                         a_date = Sys.Date()-DAYS_AGO, 
                         a_full_fn = find_full_fn_of_PT(a_pt = a_pt, a_date = a_date) ,
                         shall_I_clean_names = F,
                         ...
                         ){
  #'
  #' get the list of PTCASEID from a given file
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param a_full_fn full file name of the file to be read in
  get_df_from_url(a_full_fn, shall_I_clean_names = shall_I_clean_names, ...)  
}
# df$identifiant_de_la_personne_tsp_infocentre
# df$identifiant_de_la_personne_v10


get_pt_ids_from_file <- function(a_pt, 
                                 a_date = Sys.Date()-DAYS_AGO, 
                                 df = read_pt_file(a_pt, a_date)
                                 ){
  #'
  #' get the list of PTCASEID from a given file
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param df Dataframe that reads in from a file
  helper <- get_insert_helper(a_pt)
  
  ret_val <- df[helper$field_for_pt_case_id]
  # if (a_pt == "SK"){
  #         df["investigation_id"]
  # }else if (a_pt == "MB"){
  #   df["pt_case_id"]
  # }

  
  ret_val%>% 
    setNames("PTCaseID") %>% 
    mutate(PTCaseID = as.character(PTCaseID))
}



find_cases_inserts <- function(
  a_pt,
  a_date = Sys.Date()-DAYS_AGO, 
  keys_from_file = get_pt_ids_from_file(a_pt = a_pt, a_date = a_date),
  conn = get_covid_cases_db_con(),
  tble_nm = "case"
){
  #'
  #' get the list of PTCASEID that need to be inserted, returns a DF
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param keys_from_file vector of PTCASE IDs from a given file
  #'  @param conn This is a  connection to DB
  #'  @tble_nm conn This is table name to look fore
  
  
  
  
  # Get the columns and the fromat from the DB
  current_PT_cases <- 
    tbl(conn, tble_nm) %>% 
    filter(PT == a_pt) %>% 
    select(PTCaseID) %>% 
    collect() %>% 
    distinct() %>% 
    filter(! is.na(PTCaseID)) %>% 
    filter(trimws(PTCaseID) != "")
    
  
  keys_from_file %>% 
    anti_join(current_PT_cases, by = "PTCaseID") %>%  #view()
    filter(! is.na(PTCaseID)) %>% 
    filter(trimws(PTCaseID) != "") #%>% view()
}


get_latest_PHACID <- function(a_pt, conn = get_covid_cases_db_con(), tble_nm = "case"){
  #'
  #' get the highest  PHAC ID for any case in the DB for a given PT
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param conn This is a  connection to DB
  #'  @tble_nm conn This is table name to look fore
  
  tbl(conn, tble_nm) %>% 
    select(PHACID, PT) %>%
    filter(PT == a_pt) %>% 
    arrange(desc(PHACID)) %>% 
    head(1) %>%
    collect() %>%
    pull(PHACID)
}

get_latest_PHACID_ALL <- function(conn = get_covid_cases_db_con(), tble_nm = "case"){
  #'
  #' get the highest "Numeric" PHAC ID for any case in the DB
  #'
  #'  @param conn This is a  connection to DB
  #'  @tble_nm conn This is table name to look fore

  tbl(conn, tble_nm) %>% 
      mutate(last_PHAC_ID = as.numeric(right(PHACID, nchar(PHACID) - 3))) %>% 
    select(last_PHAC_ID, PHACID) %>%
    summarize(MAX_PHACID = max(last_PHAC_ID)) %>% 
    collect() %>%
    pull()
  
  
}

get_new_PHACIDs <- function(a_pt, n, conn = get_covid_cases_db_con()){
  #'
  #' Retrunn a vector of new phacIDs of length n
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param n number of phacids to get
  #'  @param conn This is a  connection to DB
  last_phacID <- get_latest_PHACID(a_pt, conn)
  last_Number <- get_latest_PHACID_ALL(conn)
  
  prefix <- substr(last_phacID, start = 1, stop = 3)
  #last_Number <- as.integer( substr(last_phacID, start = 4, stop = nchar(last_phacID)))
  seq(from = last_Number + 1,to = last_Number + n, by = 1) %>% 
    paste0(prefix, .)  
}

##############################
#
new_PT_cases_to_insert <- function(a_pt, conn = get_covid_cases_db_con(), a_date){
  #'
  #' Retrunn as dataframe of new records to insert
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param conn This is a  connection to DB
  
  
  newpt_cases <- find_cases_inserts(a_pt = a_pt,a_date = a_date, conn = conn) %>% pull()
  new_PHAC_IDs <- get_new_PHACIDs(a_pt = a_pt, conn = conn, n = length(newpt_cases))
  
  tibble(PHACID = new_PHAC_IDs, 
         PT =  a_pt,
         PTCaseID = newpt_cases, 
         PHACReportedDate = a_date, 
         Approved = "y")
  
}

##############################
#
Insert_new_cases_to_db <- function(a_pt, a_date, conn_func = get_covid_cases_db_con){
  #'
  #' Inserts new records into the DB but only sets the basic fields needed
  #' Of note Does not set the "classification" field
  #' 
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_date Date to run the file for
  #'  @param conn_func This is a function to use to get connection to DB
  df <- new_PT_cases_to_insert(a_pt = a_pt, a_date = a_date, conn = conn_func())

  full_fn <-find_fn_of_PT(a_pt =a_pt  , a_date = a_date)
  cases_df_inserts(full_fn = full_fn, con_fn = conn_func, df = df)  
}





format(Sys.Date(), "%b %d")

##############################
#Actually Do the thing you are supposed to do
Get_case_counts(a_pt = PT)

tic = Sys.time()
get_pt_ids_from_file(PT)
pp(format(Sys.time() - tic))


tic = Sys.time()
find_cases_inserts(PT)
pp(format(Sys.time() - tic))


tic = Sys.time()
new_PT_cases_to_insert(a_pt = PT, a_date = Sys.Date()-DAYS_AGO)
pp(format(Sys.time() - tic))


Insert_new_cases_to_db(a_pt = PT,a_date = Sys.Date()-DAYS_AGO)


Get_case_counts(a_pt = PT)
disconnect_all_covid_cases_db_con()
df_raw

source("pt_config/config_qc.R")
#df_raw <- read_pt_file(a_pt = PT, shall_I_clean = F)
path = find_full_fn_of_PT(a_pt = PT, a_date = as.Date("2020-10-18"))
cases_df <- qc$reader(path = path)
df_mapped <- qc$mapper(cases_df = cases_df)

conn = get_covid_cases_db_con()

DBI::dbWriteTable(conn = conn, name = SQL("tmp_IRIS"), value=iris)


