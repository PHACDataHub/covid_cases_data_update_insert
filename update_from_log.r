


rm(list=ls())
gc()

#options(java.parameters = "-Xmx8000m")

library(tidyverse)
library(stringr)
library(dbplyr)
library(DBI)
library(odbc)
library(keyring)
library(docstring)

DIR_OF_HPOC_ROOT = "//Ncr-a_irbv2s/IRBV2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020/"
CANADA_CASE_REPORTS = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "CANADA CASE REPORTS")

PT = "MB"
DAYS_AGO = 0


DEF_TYPE_DB = "MS_Access"
#DIR_OF_DB = file.path("~", "..", "Desktop")
DIR_OF_DB = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "DATABASE", "MS ACCESS") 
NAME_DB = "COVID-19_v2.accdb"
EXCEL_DATE_ORIGIN = "1899-12-30"



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
    #print(paste0("getting Con for ", db_type, " at ", db_full_nm))
    
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


KVP <- function(k, v, eq = "="){
  #' Key Value pair 2 string
  paste(k, eq, v)
}



L <- function(x, sep = " , ", front = "(", back = ")", qts ="'", na_replace = "null", use_nms = F, wrap_with_FB = T){
  #' 
  #' Takes a vector
  #'  - Quotes each element with qts
  #'  - seperates it with sep
  #'  - then wraps it in front and back
  
  #x[is.na(x)] <- na_replace
  
  x %>% 
    Q(qts = qts, na_replace = na_replace) %>% 
    {if(use_nms) KVP(names(.), .) else .}  %>% 
    paste0(collapse = sep) %>% 
    {if(wrap_with_FB) W(., front = front, back = back) else .}#  %>% 
    #W(front = front, back = back)
  
  
}




W <- function(str, front = "(", back = ")"){
  #' 
  #' Wraps a string with front and back
  #' 
  paste0(front,str,back)
}
Q <- function(str, qts ="'",na_replace = "null"){
  #' 
  #' Wraps string in Quotes
  #' 
  ifelse(is.na(str),
         na_replace,
    W(str,qts, qts)
  )
}


run_many_SQL_Statments <- function(all_statements, log_fn, con_fn, from_file){
  #' Executes a lot of SQL statments on a database connection
  #' 
  #' 
  
  # if (!file.exists(log_fn)){
  #   cat("has.complete, rows.affected, statement", file=log_fn, append=FALSE, sep = "\n")
  # }
  
  
  print(paste0("executing ", nrow(all_statements), " statments. Logging to '",log_fn,"'" ))
  
  # Get the results of the functions
  results <- 
    lapply(all_statements %>% pull(value), function(stmt){
      #print(stmt)
      con <- con_fn(readonly = F)
      ret_obj <- dbSendQuery(conn = con, statement = stmt)
      
      
      res <- dbGetInfo(ret_obj)
      log = list("has.complete" = res$has.completed, 
                 "rows.affected" = res$rows.affected, 
                 "date.time" = format(Sys.time()), 
                 "from_file" = from_file,
                 "statement" = Q(res$statement, qts = '"'))
      
      
      cat(L(log, wrap_with_FB = F, qts = ""), file=log_fn, append=TRUE, sep = "\n")
      
      cat(".")
      
      dbClearResult(ret_obj)
      
      dbDisconnect(con)
      return(log)
    }) %>%bind_rows()
  
  #log_fn <- file.path(CANADA_CASE_REPORTS, a_pt, paste0(fn, ".sql_log_run_", Sys.Date(), ".csv"))
  #write_csv(x = results, log_fn, col_names = T)
  return(results)
}


cases_df_updates <- function(
  a_pt = PT,
  a_days_ago = DAYS_AGO,
  fn = paste0(a_pt, "_log_", format(Sys.Date()- a_days_ago,"%m%d"), ".xlsx"),
  full_fn = file.path(CANADA_CASE_REPORTS, a_pt, paste0(a_pt, format(Sys.Date()- a_days_ago,"%m%d")),  fn),
  df = readxl::read_xlsx(full_fn) %>% clean_names(),
  con_fn = get_covid_cases_db_con
  ){
  #'   
  #' reads in a Data frame and executes it
  #'  performs all needed updates to a database
  #'  

  if(nrow(df) == 0){
    warning(paste0("There are no updated records for '", a_pt, "'"))
    return(NULL)
  }
  
  
 
  df$app <- df$approved %>% trimws() %>%  str_sub(1,1) %>% str_to_lower()
  
  
  # Make sure there are not NAs in the "Approved" section
  if (df$approved %>% is.na() %>% any()){
    warning("The approved collumn must entirely start with yes or no.")
    return(NULL)
  }

  
  # Everything has to be either approved or not
  if ((!df$app %in% c("y", "n")) %>% any()){
    warning("The approved collumn must entirely start with y or n.")
    return(NULL)
  }
  
  
  # just the yes  
  df_y <- df %>% filter(app == "y") 
  
  
  
  if(nrow(df_y) == 0){
    warning(paste0("There are no approved updates for ", a_pt))
    return(NULL)
  }  
  
  
  
  #if the column name is "DELETED", we do a special thing
  df_y <- 
  df_y %>% 
    mutate(new_value = ifelse(var_name == "Deleted",
                           "PT Deleted", new_value),
           old_value = ifelse(var_name == "Deleted",
                              "not known to this script", old_value)
           )  %>% 
    mutate(var_name = ifelse(var_name == "Deleted",
                             "Classification", var_name)
           )
  
  
  

  
  
  
  
  
  
  #vars to update
  vars <- df_y$var_name %>% unique()
  
  
  tble_nm = "Case"
  
  # Get the statments to execute
  all_statements <- 
    lapply(vars, function(col_nm){
    

      
      
      # for this particular column
      df_var_nm <- df_y %>% filter(var_name == col_nm) 
      
      
      # what are the unique values to set to ?
      uvals <- df_var_nm %>% distinct(new_value) %>% pull()
      
      

      
      
      
      ret_val <- 
        lapply(uvals, 
               function(curr_val){  
                
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
  
  
  #log_fn = file.path(CANADA_CASE_REPORTS, a_pt, paste0(fn, ".sql_log_run_", Sys.Date(), ".csv"))
  log_fn = file.path(DIR_OF_DB, paste0(NAME_DB , ".log.csv"))

  # Get the results of the functions
  results <- 
    run_many_SQL_Statments(all_statements = all_statements, 
                         log_fn = log_fn, 
                         from_file = fn,
                         con_fn = con_fn)
 
  
  print(paste0("updated '", nrow(results) , "' statements for '", a_pt, "'. Check log file at \n'", log_fn,"'"))
  
  
  return(TRUE)
}


cases_df_inserts <- function(
  a_pt = PT,
  a_days_ago = DAYS_AGO,
  fn = paste0(a_pt, "_insert_", format(Sys.Date()- a_days_ago,"%m%d"), ".xlsx"),
  full_fn = file.path(CANADA_CASE_REPORTS, a_pt, paste0(a_pt, format(Sys.Date()- a_days_ago,"%m%d")), fn),
  df = readxl::read_xlsx(full_fn, col_types = "text") ,#%>% clean_names(),
  con_fn = get_covid_cases_db_con
){
 #' Reads a log file with approvals
 #' Executes changes to a DB
 #' logs the results
 #'
 #'
 #'

  tble_nm = "Case"

  
  

  df_new <- df #%>% filter(PHACReportedDate == Sys.Date() - 1)

  #df_new$approved <- "y"
  
  if(nrow(df_new) == 0){
    warning(paste0("There are no inserted records for '", a_pt, "'"))
    return(NULL)
  }
  
  

  df_new$app <- df_new$Approved %>% trimws() %>%  str_sub(1,1) %>% str_to_lower()


  

  # Make sure there are not NAs in the "Approved" section
  if (df_new$Approved %>% is.na() %>% any()){
    warning("The approved collumn must entirely start with yes or no.")
    return(NULL)
  }


  # Everything has to be either approved or not
  if ((!df_new$app %in% c("y", "n")) %>% any()){
    warning("The approved collumn must entirely start with y or n.")
    return(NULL)
  }

  
  
  df_y <- df_new %>% 
    filter(app == "y") %>% 
    select(-Approved, -app) %>% 
    mutate_if(is.character, function(x){
      x[is.na(x)] <- ""
      return(x)
    })
  #df_y <- df_y %>% mutate_if(is.logical, as.character)
  
  
  if(nrow(df_y) == 0){
    warning(paste0("There are no approved inserted records for ", a_pt))
    return(NULL)
  }  
  
  
  # Get the columns and the fromat from the DB
  tbl_format <- tbl(con_fn(), tble_nm) %>% head(1) %>% collect() %>% slice(0)
  
  # Make sure we have compatible types and format from our DF
  lapply(colnames(tbl_format), function(col_nm){
    #print(col_nm)
    cls <- class(tbl_format[[col_nm]])
    cls <- cls[[1]]
    #print(cls)
    
    if (is.null(df_y[[col_nm]])){
      df_y[[col_nm]] <- ""
    }
    if ( cls == "POSIXct" ){
      df_y[[col_nm]] <<- as.Date(as.integer(df_y[[col_nm]]), origin = EXCEL_DATE_ORIGIN)
    }else{
      df_y[[col_nm]] <<- as(df_y[[col_nm]], cls)
    }
    return(TRUE)
  })
  
  
  
  nms <- df_y %>% colnames() %>% L(qts = "")
  stmnt_base <- paste0("INSERT INTO " ,tble_nm, " ", nms, " VALUES ")

  all_statements <-
  lapply(1:nrow(df_y), function(irow){

    curr_row <- df_y %>%
        slice(irow)
    
    stmnt <-
      stmnt_base %>% 
      paste0(  
        lapply(colnames(curr_row), function(col_nm){
          as.character(curr_row[[col_nm]])
        }) %>%
        unlist(use.names=FALSE) %>%
        L()
      )
    return(stmnt)
  })  %>% unlist() %>%  as_tibble()


  # Get the results of the functions
  
  #log_fn = file.path(CANADA_CASE_REPORTS, a_pt, paste0(fn, ".sql_log_run_", Sys.Date(), ".csv"))
  log_fn = file.path(DIR_OF_DB, paste0(NAME_DB , ".log.csv"))
  results <- 
    run_many_SQL_Statments(all_statements = all_statements, 
                           log_fn = log_fn, 
                           from_file = fn,
                           con_fn = con_fn)
  # 
  print(paste0("Inserted '", nrow(results) , "' cases for '", a_pt, "'. Check log file at \n'", log_fn,"'"))
  
}

Get_case_counts <- function(a_pt = PT,
                            con = get_covid_cases_db_con()){
  
  tbl(con, "Case") %>% 
   filter(PT == a_pt) %>% 
   filter(Classification %in% c("Confirmed", "Probable")) %>% 
   tally() %>% pull() %>% format() %>% 
    paste0("After Additions deletions and updates there are ", . , " cases in ", a_pt) 
  
}


cases_df_inserts()
cases_df_updates()
Get_case_counts()






