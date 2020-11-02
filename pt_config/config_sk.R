# Saskatchewan
sk <- list(
    name = "sk",
    code = "47",
    
    reader = function(path) {
        readxl::read_xlsx(path) %>%
            set_names(str_to_lower(names(.))) %>% # set column names to lower case
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },
    
    mapper = function(cases_df){ 
        cases_df %>%
            rename(ptcaseid = `investigation id`, #4 are not unique?
                   gender = `client gender`,
                   closecontactcase = `close contact`
                   ) %>%
            mutate(pt = "sk") %>% 
            #should "Community" (y/n/u) map to exposure? 
            mutate(reporteddate = if_else(`phac date type` == 'date reported', `phac date`, NULL)) %>% 
            mutate(onsetdate = if_else(`phac date type` == 'symptom onset', 
                                       `phac date`, 
                                       NULL)) %>%
            mutate(disposition = if_else(deceased == 'y', 
                                         "deceased", 
                                         if_else(recovered == 'y', 
                                                 "recovered", 
                                                 NULL))) %>% 
            mutate(labspecimencollectiondate = if_else(`phac date type` == 'specimen collection', 
                                                       `phac date`, 
                                                       NULL)) %>%
            mutate(labtestresultdate = if_else(`phac date type` == 'lab test result', `phac date`, NULL)) %>%
            mutate(closecontactcase = recode(closecontactcase, 
                                             "y" = "yes", "n" = "no", "u" = "unknown")) %>%
            mutate(isolation = recode(isolation, "y" = "yes", "n" = "no")) %>%
            mutate(hosp = recode(hospitalization, "y" = "yes", "n" = "no")) %>% 
            mutate(travel_domestic = recode(`domestic travel`, "not required" = "unknown")) %>% 
            mutate(travel_international = recode(`international travel`, "not asked" = "unknown")) %>% 
            mutate(testingreason = "not collected") %>% 
            mutate(restingreasonspec = "not collected") %>% 
            mutate(racespec = "not collected") %>% 
            mutate(indigenousidentity = "not collected") %>% 
            mutate(indigenousidentityspec = "not collected") %>% 
            mutate(residence_reservecommunitycrownland = "not collected") %>% 
            mutate(rfhypertension = "not collected") %>% 
            mutate(rfchronickidneydisease = "not collected") %>% 
            mutate(rfcopd = "not collected") %>% 
            mutate(rfasthma = "not collected") %>% 
            mutate(rfliverdisease = "not collected") %>% 
            mutate(rfsicklecelldisease = "not collected") %>% 
            mutate(rfcerebrovasculardisease = "not collected") %>% 
            mutate(rfpregtrimester = "not collected") %>% 
            mutate(rfpostpartum = "not collected") %>% 
            mutate(rfvaping = "not collected") %>% 
            mutate(rfproblematicsubstanceuse = "not collected") %>% 
            mutate(closecontactother = "not collected") %>% 
            mutate(closecontactotherspec = "not collected") %>% 
            mutate(cluster = "not collected") %>% 
            mutate(outbreakid = "not collected") %>% 
            mutate(numberofcontacts = "not collected") %>%
            mutate(exposuresetting_otherspec = "not collected") %>%
            mutate(exposuresettingyype_otherspec = "not collected") %>%
            mutate(sequencing = "not collected") %>%
            mutate(labid = "not collected") %>%
            mutate(lab_name = "not collected") 
    })
