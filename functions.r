


source("utils.r")



packages.get(c(
  "tidyverse",
  "stringr",
  "dbplyr",
  "DBI",
  "odbc",
  "docstring",
  "janitor"
))


G_LIST_CON = list()
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
  #' Get a connection for the data base
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
                     .connection_string = con_str)#, encoding = "latin1")
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
  
  G_LIST_CON[length(G_LIST_CON) + 1] <<- con
  return(con)  
}

##################################################
# JUST Testing to see if we can get a connection
#get_covid_cases_db_con()


disconnect_all_covid_cases_db_con <- function(){
  if (length(G_LIST_CON)  == 0 )
    return(TRUE)
  
  results <- 
  lapply(length(G_LIST_CON):1, function(i){ 
    dbDisconnect(G_LIST_CON[[i]])
    G_LIST_CON[i] <<- NULL
    
  })
  return(T)
}



####################################
#
KVP <- function(k, v, eq = "="){
  #' Key Value pair to a string
  paste(k, eq, v)
}




####################################
#
clean_str <- function(x, 
                      pattern = "[[:punct:]]", 
                      replacement = " ", 
                      NA_replace = "", 
                      BLANK_replace = "",
                      capitalize_function = str_to_title){
  #' By default this replactes all punctuation, and trims spaces so there is only one
  #' then sets title case
  #' 
  x %>% str_replace_all(pattern = pattern, replacement = replacement) %>%
    str_replace_all(pattern = "[ ]+", replacement = " ") %>%
    ifelse(is.na(.), NA_replace, .) %>% 
    #str_to_lower() %>% 
    capitalize_function() %>% 
    trimws() %>%  #%>% table()
    ifelse(. == "", BLANK_replace, .) 
}


####################################
#
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



####################################
#
W <- function(str, front = "(", back = ")"){
  #' 
  #' Wraps a string with front and back
  #' 
  paste0(front,str,back)
}


####################################
#
Q <- function(str, qts ="'",na_replace = "null", escape = T){
  #' 
  #' Wraps string in Quotes
  #' 

  
    # ifelse(is.na(str),
    #      na_replace, 
    #      str) %>% 
    str %>%
    {if(escape) gsub(perl = T, x = ., pattern = "'", replacement = "''") else .} %>%
    W(.,qts, qts) %>%
    ifelse(is.na(str), na_replace,.)
    # ifelse(test = is.na(str) , yes =  na_replace , no = W(.,qts, qts)   
    # )
}




####################################
#
run_many_SQL_Statments <- function(all_statements, log_fn, con_fn, from_file){
  #' Executes a lot of SQL statments on a database connection
  #' 
  #' @param all_statements a dataframe with column named "value" that contains some SQL statments
  #' @param log_fn the file name that we will log DB changes to
  #' @param con_fn the function that we call that gets a connection to the DB
  #' @param from_file string indicating the file the changes were read in from
  
  
  print(paste0("executing ", nrow(all_statements), " statments. Logging to '",log_fn,"'" ))
  
  # Get the results of the functions
  
  i = 1
  results <- 
    lapply(all_statements %>% pull(value), function(stmt){
      #print(stmt)
      Encoding(stmt) <- "UTF-8"
      con <- con_fn(readonly = F)
      ret_obj <- dbSendQuery(conn = con, statement = stmt)
      
      
      
      res <- dbGetInfo(ret_obj)
      log = list("has.complete" = res$has.completed, 
                 "rows.affected" = res$rows.affected, 
                 "date.time" = format(Sys.time()), 
                 "from_file" = from_file,
                 "statement" = Q(res$statement, qts = '"'))
      
      
      cat(L(x = log, wrap_with_FB = F, qts = ""), file=log_fn, append=TRUE, sep = "\n")
      
      cat(paste0(i, ","))
      i<<-i+1
      
      dbClearResult(ret_obj)
      
      dbDisconnect(con)
      
      
      return(log)
    }) %>%bind_rows()
  
  #log_fn <- file.path(CANADA_CASE_REPORTS, a_pt, paste0(fn, ".sql_log_run_", Sys.Date(), ".csv"))
  #write_csv(x = results, log_fn, col_names = T)
  return(results)
}
################################





get_txt_thing <- function(
  a_pt = PT,
  a_type = "_log_",
  a_days_ago = DAYS_AGO,
  a_file_suffix = FILE_SUFFIX,
  a_canada_case_reports = CANADA_CASE_REPORTS  
){
  #' Get a text string that is commonly used
  format(Sys.Date()- a_days_ago,"%m%d") %>% 
    paste0(a_pt, a_type,.)
}


get_update_txt_thing <- function(...){
  #' Get a text string that is commonly used for updates
  get_txt_thing(..., a_type = "_log_")
  
}
get_insert_txt_thing <- function(...){
  #' Get a text string that is commonly used for inserts
  get_txt_thing(..., a_type = "_insert_")
  
}

####################################
#
get_full_fn_for_updates <- function(
  a_pt = PT,
  a_file_suffix = FILE_SUFFIX,
  a_canada_case_reports = CANADA_CASE_REPORTS,
  ...
){
  #' 
  #' 
  #' Returns the full file name of the updates file
  #' 
  #' 
  #' 
  get_update_txt_thing(a_pt = a_pt, ...) %>% 
    paste0(a_file_suffix ,".xlsx") %>%
    file.path(a_canada_case_reports, a_pt, .)
  
}
get_full_fn_for_updates()






####################################
#
cases_df_updates <- function(
  full_fn = get_full_fn_for_updates(),
  df = readxl::read_xlsx(path = full_fn, 
                         sheet = get_update_txt_thing()) %>% clean_names(),
  con_fn = get_covid_cases_db_con,
  a_tble_nm = "case"
){
  #'   
  #'  reads in a Data frame and executes it
  #'  performs all needed updates to a database
  #'  records updates to a log file
  #'  
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_days_ago how many days ago this file came into PHAC
  #'  @param fn name of the file to proccess 
  #'  @param full_fn full file name to proccess 
  #'  @param df dataframe to proccess
  #'  @param con_fn connection function to call when we need a DB connection
  
  
  
  
  #If there are no records to update that is fine
  if(nrow(df) == 0){
    print(paste0("There are no updated records for "))
    return(TRUE)
  }
  
  
  
  # Make sure there are not NAs in the "Approved" section
  if (df$approved %>% is.na() %>% any()){
    warning("The approved collumn must entirely start with yes or no.")
    return(NULL)
  }
  
  
  
  #just look for the y
  df$app <- df$approved %>% trimws() %>%  str_sub(1,1) %>% str_to_lower()
  
  
  # Everything has to be either approved or not
  if ((!df$app %in% c("y", "n")) %>% any()){
    warning("The approved collumn must entirely start with y or n.")
    return(NULL)
  }
  
  df <- df %>% mutate(new_value = as.character(new_value))
  #utf 8 encoding seems to work but we need to change the DB to allow for UTF encodings
  df %>% mutate(e = Encoding(new_value)) %>% select(new_value, e)
  Encoding(df[["new_value"]])<- 'UTF-8'
  
  
  
  
  # just the yes  
  df_y <- df %>% filter(app == "y") 
  
  
  
  #there were no approved updates and that is FINE!!!!!
  if(nrow(df_y) == 0){
    warning(paste0("There are no approved updates for this PT"))
    return(TRUE)
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
  
  
  #if the new name is "Remove", we do a special thing
  df_y <- 
    df_y %>% 
    mutate(new_value = ifelse(new_value == "*remove*",
                              "", new_value)
    ) 
  
  
  #if the new value is NA we do a special thing
  df_y <- 
    df_y %>% 
    mutate(new_value = ifelse(is.na(new_value),
                              "", new_value)
    ) 
  
  
  #vars to update
  vars <- df_y$var_name %>% unique()
  
  tble_nm = a_tble_nm
  
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
                     
                     if(length(ids) < 50){
                       paste0(sql_strt, Q(curr_val)," where phacid in ", L(ids))
                     }else{
                       ids_chunk <- split(ids, ceiling(seq_along(ids)/50))
                       lapply(ids_chunk, function(chnk){paste0(sql_strt, Q(curr_val)," where phacid in ", L(chnk))}) %>% unlist(use.names = F)
                     }
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
                           from_file = basename(full_fn)
                           ,
                           con_fn = con_fn)
  
  print(paste0("updated '", nrow(results) , "' statements. Check log file at \n'", log_fn,"'"))
  return(TRUE)
}







####################################
#
get_full_fn_for_inserts <- function(
  a_pt = PT,
  a_file_suffix = FILE_SUFFIX,
  a_canada_case_reports = CANADA_CASE_REPORTS,
  ...
){
  #' 
  #' 
  #' Returns the full file name of the updates file
  #' 
  #' 
  #' 
  get_insert_txt_thing(a_pt = a_pt, ...) %>% 
    paste0(a_file_suffix ,".xlsx") %>%
    file.path(a_canada_case_reports, a_pt, .)
  
}
get_full_fn_for_inserts()









####################################
#
cases_df_inserts <- function(
  full_fn = get_full_fn_for_inserts(),
  df = readxl::read_xlsx(path = full_fn, 
                         col_types = "text", 
                         sheet = get_insert_txt_thing()
  ) ,#%>% clean_names(),
  con_fn = get_covid_cases_db_con,
  a_tble_nm = "case"
){
  #' Reads a log file with approvals
  #' Executes changes to a DB
  #' logs the results
  #'  
  #'  @param a_pt two letter string like QC, ON BC etc....
  #'  @param a_days_ago how many days ago this file came into PHAC
  #'  @param fn name of the file to proccess 
  #'  @param full_fn full file name to proccess 
  #'  @param df dataframe to proccess
  #'  @param con_fn connection function to call when we need a DB connection
  
  tble_nm = a_tble_nm
  
  df_new <- df #%>% filter(PHACReportedDate == Sys.Date() - 1)
  
  # there are no inserts and that is OK! better then OK really!!!!
  if(nrow(df_new) == 0){
    print(paste0("There are no inserted records for this PT"))
    return(TRUE)
  }
  
  
  #just worry about the y and n
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
  #Get only the approved stuffs
  df_y <- df_new %>% 
    filter(app == "y") %>% 
    select(-Approved, -app) %>% 
    mutate_if(is.character, function(x){
      x[is.na(x)] <- ""
      return(x)
    })
  
  #There are no approved inserts and that is fine!
  if(nrow(df_y) == 0){
    warning(paste0("There are no approved inserted records for this PT"))
    return(TRUE)
  }  
  
  
  #Remove useless columns
  df_y <- 
    df_y %>% select_if(function(x) 
      any(!(is.na(x) | as.character(x) == ""))
    )

  # Get the columns and the fromat from the DB
  tbl_format <- tbl(con_fn(), tble_nm) %>% head(1) %>% collect() %>% slice(0)
  
  # Make sure we have compatible types and format from our DF
  lapply(colnames(tbl_format), function(col_nm){
    #print(col_nm)
    cls <- class(tbl_format[[col_nm]])
    cls <- cls[[1]]
    #print(cls)
    
    
    if (! is.null(df_y[[col_nm]])){
      if ( cls == "POSIXct" ){
        if(class(df_y[[col_nm]]) != "Date"){
          df_y[[col_nm]] <<- as.Date(as.integer(df_y[[col_nm]]), origin = EXCEL_DATE_ORIGIN)
        }
      }else{
        df_y[[col_nm]] <<- as(df_y[[col_nm]], cls)
      }
    }
    return(TRUE)
  })
  
  
  #########################################################
  # # This method is quicker but does not log the inserts
  # print(paste0("Beginning Insert of ", nrow(df_y), " records, with ", ncol(df_y), " columns into '", tble_nm, "' for PT ='", a_pt))
  # print(paste0("Current time = ", format(Sys.time())))
  # print(paste0("Rough estimate at 2 seconds per record ", 2*nrow(df_y)/60, " minutes"))
  # conn = con_fn(readonly = F)
  # ret_obj <- dbWriteTable(conn = conn, name = tble_nm, value = df_y, batch_rows = 1, overwrite = F, append = T)
  # 
  # return(ret_obj)
  #########################################################
  
  
  #########################################################
  # This issert method is slower but it will log the inserts
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
  
  
  
  ###########################################
  # 
  log_fn = file.path(DIR_OF_DB, paste0(NAME_DB , ".log.csv"))
  
  
  ###########################################
  # Get the results of the SQL 
  results <-
    run_many_SQL_Statments(all_statements = all_statements,
                           log_fn = log_fn,
                           from_file = basename(full_fn),
                           con_fn = con_fn)
  
  
  ##############################################
  # print results for user
  print(paste0("Inserted '", nrow(results) , "' cases. Check log file at \n'", log_fn,"'"))
  return(TRUE)
}







##############################################
#
Get_case_counts <- function(a_pt = PT,
                            con = get_covid_cases_db_con()){
  #'
  #' Say some stats about this province
  #'
  #' @param a_pt two letter string like QC, ON BC etc....
  #' @con = a connection to the DB we need to use
  
  tbl(con, "Case") %>% 
    filter(PT == a_pt) %>% 
    filter(Classification %in% c("Confirmed", "Probable")) %>% 
    tally() %>% pull() %>% format() %>% 
    paste0("Currently there are ", . , " cases in ", a_pt) 
}


