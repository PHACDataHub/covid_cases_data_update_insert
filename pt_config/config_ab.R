#Alberta
ab <- list(
    name = "ab",
    code = "48",
    
    reader = function(path) {
        col_types <- cols(.default = col_character(), 
                          LabSpecimenCollectionDate = col_date(),
                          LabTestResultDate = col_date(),
                          ReportedDate = col_date(),
                          Age = col_double(),
                          OnsetDate = col_date(),
                          NumberofContacts = col_double(),
                          Travel = col_double(),
                          Hosp = col_double(),
                          ICU = col_double(),
                          death = col_double(),
                          DeathDate = col_date(),
                          RecoveryDate = col_date())
        read_csv(path, col_types = col_types) %>%
            set_names(str_to_lower(names(.))) %>% # set column names to lower case
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },
    
    mapper = function(cases_df){
        cases_df %>% 
            rename(occupation = occupationtype,
                   cluster = outbreakassociated) %>% 
            mutate(hosp = recode(hosp, "1" = "yes", "0" = "no")) %>%
            mutate(icu = recode(icu, "1" = "yes", "0" = "no")) %>%
            mutate(travel = recode(travel, "1" = "yes", "0" = "no" )) %>% #also traveltype
            mutate(travel = if_else(exposuresettingtype == "travel", "yes", travel)) %>%
            mutate(travel_domestic = if_else(traveltype == "domestic only", "yes", NULL)) %>% 
            mutate(travel_international = if_else(str_detect(traveltype, "international"), "yes", NULL)) %>% 
            mutate(coviddeath = recode(death, "1" = "yes", "0" = "no")) %>% 
            mutate(ltc_resident = if_else(dwellingtype == "long term care facility", "yes", NULL)) %>%
            mutate(occupationhcw = if_else(occupation == "healthcare worker", "yes", NULL)) %>% 
            rename(travel1_international_loc = matches("travel_country_[0-9]{1-2}")) %>%
            rename(travel2_international_loc = matches("travel_origcountry_[0-9]{1-2}")) %>%
            mutate(across(matches("travel[0-9]{1}"), parse_location)) %>% #just to standardize country names slightly
            mutate(occupationhcwrolespec = "not collected") %>% 
            mutate(occupationhcwpatientcare = "not collected")
    }
)



