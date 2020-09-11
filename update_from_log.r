


rm(list=ls())
gc()

#options(java.parameters = "-Xmx8000m")

library(tidyverse)
library(janitor)
library(stringr)
library(dbplyr)
library(DBI)
library(odbc)
library(keyring)
library(docstring)

BASE_COVID_DIR = "//Ncr-a_irbv2s/IRBV2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020/"
CANADA_CASE_REPORTS = file.path(BASE_COVID_DIR, "DATA AND ANALYSIS", "CANADA CASE REPORTS")

PT = "AB"
DAYS_AGO = 1


DEF_TYPE_DB = "MS_Access"
DIR_OF_DB = file.path("~", "..", "Desktop") 
NAME_DB = "COVID-19_v2.accdb"




####################################
#
get_covid_cases_db_con <- function(
  db_type = DEF_TYPE_DB, 
  db_dir = DIR_OF_DB,
  db_name = NAME_DB,
  db_full_nm = path.expand(file.path(db_dir, db_name)),
  db_pwd = keyring::key_get("COVID-19"),
  readonly = T
)
{
  #' Get a connection fo the data base
  #' 
  #' @param db_type type of DB SQLite MSAccess Postgres etc....
  #' @param db_dir for MS Access ans SQLite
  #' @param db_name for MS Access ans SQLite
  #' @param db_full_nm for MS Access ans SQLite
  #' @param db_pwd password for the database
  #' 
  con <- NULL
  if (db_type == "MS_Access"){
    print(paste0("getting Con for ", db_type, " at ", db_full_nm))
    
    con_str = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", 
                     db_full_nm, 
                     ";PWD=", db_pwd, ";")
    
    if (readonly == T){
      con_str <- paste0(con_str , "ReadOnly=1;applicationintent=readonly;")
      
    }
    con <- dbConnect(odbc::odbc(),
                     .connection_string = con_str)
  }else if (db_type == "xlsx"){
    #TODO : ODBC is having issues on Jenne compuiter so we pretend its a DB connection
    return(db_full_nm)
    # con <- dbConnect(odbc::odbc(),
    #                  .connection_string = paste0("Driver={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=",db_full_nm,";ReadOnly=0;"))
    
  }
  else
  {
    warning(paste0("Unknown db_type='", db_type, "'. Returning NULL from get_covid_cases_db_con"))
  }
  return(con)  
}

L <- function(x, sep = " , ", front = "(", back = ")", qts ="'"){
  x %>% 
    Q(qts = qts) %>% 
    paste0(collapse = sep) %>% 
    W(front = front, back = back)
}
W <- function(str, front = "(", back = ")"){
  paste0(front,str,back)
}
Q <- function(str, qts ="'"){
  W(str,qts, qts)
}

cases_df_updates <- function(
  a_pt = PT,
  a_days_ago = DAYS_AGO,
  fn = paste0(a_pt, "_log_", format(Sys.Date()- a_days_ago,"%m%d"), ".xlsx"),
  full_fn = file.path(CANADA_CASE_REPORTS, a_pt, fn),
  df = readxl::read_xlsx(full_fn) %>% clean_names(),
  con_fn = get_covid_cases_db_con
  ){
  #' performs all needed updates to a database

 
  df$app <- df$approved %>% trimws() %>%  str_sub(1,1) %>% str_to_lower()
  
  
  if (df$approved %>% is.na() %>% any()){
    warning("The approved collumn must entirely start with yes or no.")
    return(NULL)
  }

  
  if ((!df$app %in% c("y", "n")) %>% any()){
    warning("The approved collumn must entirely start with y or n.")
    return(NULL)
  }
  
  
  
  df_y <- df %>% filter(app == "y") 
  
  vars <- df_y$var_name %>% unique()
  
  tble_nm = "Case"
  
  
  all_statements <- 
    lapply(vars, function(col_nm){
    
      
      df_var_nm <- df_y %>% filter(var_name == col_nm) 
      uvals <- df_var_nm %>% distinct(new_value) %>% pull()
      
      
      ret_val <- 
        lapply(uvals, function(curr_val){  
        
      ids <- df_var_nm %>% 
          filter(new_value == curr_val) %>% 
          distinct(phacid) %>% pull()
      
      sql_strt <- paste0("UPDATE ",tble_nm," SET ",  col_nm, " = ")
      
      update_statement <-
      if (length(ids)== 1){
        paste0(sql_strt, Q(curr_val)," where phacid = ", Q(ids))
        #sql_strt("UPDATE Case SET ",  col_nm, " = ", curr_val," where phacid = ", ids, con = con)
      } else if (length(ids) > 1){
        paste0(sql_strt, Q(curr_val)," where phacid in ", L(ids))
        #build_sql("UPDATE Case SET ",  col_nm, " = ", curr_val," where phacid in ", ids, con = con)
      } else{
        NULL
      }
      
      return(update_statement)
        
      
      }) %>% unlist() %>% as_tibble()
      
      return(ret_val)
    }) %>% bind_rows()
  
  
  lapply(all_statements %>% pull(value), function(stmt){
    print(stmt)
    dbSendQuery(conn = con_fn(readonly = F), statement = stmt) 
  })
  
  
  
}

cases_df_updates()







