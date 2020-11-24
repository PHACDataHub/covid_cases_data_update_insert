


rm(list=ls())
gc()




DIR_OF_HPOC_ROOT = "//Ncr-a_irbv2s/IRBV2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020/"
CANADA_CASE_REPORTS = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "CANADA CASE REPORTS")

PT = "NS"
DAYS_AGO = 0


# we can add a suffix for weekly files
#FILE_SUFFIX = "_weekly"
FILE_SUFFIX = ""


DEF_TYPE_DB = "MS_Access"


##################################
# If updates are small you can leave DB on network drive
#DIR_OF_DB = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "DATABASE", "MS ACCESS") 

##################################
# FOR LARGE UPDATES LIKE QC or AB or even ON, copy to desktop and run 
DIR_OF_DB = file.path("~", "..", "Desktop") 


NAME_DB = "COVID-19_v2.accdb"
EXCEL_DATE_ORIGIN = "1899-12-30"












source("functions.r")





TESTING_LOCAL = FALSE
if (TESTING_LOCAL){
  DIR_OF_DB = file.path("~", "..", "Desktop")
  CANADA_CASE_REPORTS = file.path("~", "..", "Desktop", "input")
}








##############################################
#
do_update_insert_delete <- function(...){
  #'
  #' Do all the inserts deletions and updates for the province
  #'
  print("before Running updates and Inserts")
  print(Get_case_counts())
  
  tic = Sys.time()
  ret_val <- cases_df_inserts(...)
  print(paste("inserts took ", format(Sys.time() - tic)))
  
  tic = Sys.time()
  ret_val <- if (!is.null(ret_val)) cases_df_updates(...)
  print(paste("updates took ", format(Sys.time() - tic)))
  
  if (!is.null(ret_val)) print(Get_case_counts(...))
  else print(paste0("there seems to be some issue with insert or update..."))
}



############################################
#actually do something
do_update_insert_delete()

#########################
#Counts fo the PT 
#Get_case_counts()

#########################
#just the inserts
#cases_df_inserts()


#########################
#just the updates
#cases_df_updates()


# cases_df_updates( full_fn = file.path("~", "..", "Desktop","ttfn.xlsx"),
#   df = readxl::read_xlsx(path = full_fn) %>% clean_names()
# )



#Get_case_counts()
disconnect_all_covid_cases_db_con()
