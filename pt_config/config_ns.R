#Nova Scotia
ns <- list(
    name = "ns",
    code = "12",
    
    reader = function(path) {
        readxl::read_xlsx(path) %>%
            set_names(str_to_lower(names(.))) %>% # set names to lowercase
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    }, 
    
    mapper = function(cases_df){
        cases_df %>% 
            rename(ptcaseid = pt_id, 
                   episodetype = episode_date_type, 
                   classification = case_classification,
                   gender = sex) %>% 
            mutate(ageunit = "years") %>% 
            mutate(pt = "ns") %>% 
            mutate(onsetdate = if_else(episodetype == "symptom onset", episodedate, onsetdate)) %>% 
            mutate(reporteddate = if_else(episodetype == "date reported", episodedate, NULL)) %>% 
            mutate(labspecimencollectiondate = if_else(episodetype=="specimen collection", episodedate, labspecimencollectiondate1)) %>% 
            mutate(labtestresultdate = if_else(episodetype =="clinical diagnosis", episodedate, labtestresultdate1)) %>% 
            mutate(classification = recode(classification, "case - confirmed" = "confirmed", "case - probable" = "probable")) %>% 
            mutate(healthcare_worker = if_else(occupation == "healthcare worker/volunteer with direct patient contact", "yes", if_else(occupationspec == "health care facility - work/volunteer", "yes", NULL))) %>% #check "no" condition
            mutate(ltc_resident = if_else(occupation == "resident of long-term care facility/institutional facility", "yes", if_else(occupationspec == "long-term care facility - work/volunteer/reside", "yes", NULL))) %>% 
            mutate(occupation_other = if_else(occupation == "school or daycare worker/attendee", occupation, if_else(occupationspec == "child care setting- work/volunteer/attend", "school or daycare worker/attendee",if_else(occupation == "farm worker", occupation, if_else(occupation == "animal handler or setting- work/volunteer/attend/reside", "farm worker", NULL))))) %>% 
            mutate(closecontactcase = if_else(transmission == "close contact" | transmission == "close contact/travel", "yes", closecontactcase)) %>% 
            mutate(exposuresetting = if_else(transmission == "community", "community unknown source", if_else(transmission == "travel", "travel tourism", NULL))) %>%  # may need to add more categories 
            mutate(travel = if_else(transmission == "travel", "yes", travel)) %>% 
            mutate(testingreason = "not collected") %>% 
            mutate(testingreasonspec = "not collected") %>% 
            mutate(healthregion = "not collected") %>% 
            mutate(occupationhcwrole = "not collected") %>% 
            mutate(occupationhcwpatientcare = "not collected") %>% 
            mutate(rfhypertension = "not collected") %>% 
            mutate(rfcopd = "not collected") %>% 
            mutate(rfasthma = "not collected") %>% 
            mutate(rfcerebrovasculardisease = "not collected") %>% 
            mutate(rfpregtrimester = "not collected") %>% 
            mutate(rfotherspec = "not collected") %>% 
            mutate(travel_domestic_loc = "not collected") %>% 
            mutate(travel_international_loc = "not collected") %>% 
            mutate(closecontactother = "not collected") %>% 
            mutate(closecontactctherspec = "not collected") %>%
            mutate(exposuresetting_otherspec = "not collected") %>%
            mutate(exposuresettingtype_otherspec = "not collected") 
            
            
    }
)

