# Ontario
on <- list(
    name = "on",
    code = "35",
    
    reader = function(path) {
        # we have to read some ids as numeric otherwise if we read them as text excel will convert it to scientific notation
        # we then cast everything to character for uniformity and ease of comparisons later in the mapper
        col_types = c("text","text","text","numeric","numeric", "text", "text","text","date","text","date","text","text", "numeric","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text","text", "text", "text")
        readxl::read_xlsx(path, sheet = "LineList", col_types = col_types) %>%
            set_names(str_to_lower(names(.))) %>% # set column names to lower case
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },
    
    mapper = function(cases_df){ 
        cases_df %>% 
            rename(healthregion = diagnosing_health_unit_area_desc,
                   classification = classification_description,
                   age = age_at_time_of_illness,
                   onsetdate = symptomonsetdate) %>%
            mutate(ptcaseid = if_else(!is.na(case_id_iphis), as.character(case_id_iphis), case_id)) %>%
            mutate(ageunit = "years") %>% 
            mutate(pt = "on") %>% 
            mutate(occupationhcw = recode(hcw, "unk" = "unknown", "not" = "no")) %>% 
            mutate(reporteddate = if_else(episode_date_type %in% 
                                              c("reported", "onset"), 
                                          accurate_episode_date, 
                                          NULL)) %>%   #or map to symptomOnsetDate
            mutate(labspecimencollectiondate = if_else(episode_date_type == "specimen", 
                                                       accurate_episode_date, NULL)) %>%
            mutate(labtestresultdate = if_else(episode_date_type == "lab result", 
                                               accurate_episode_date, NULL)) %>% 
            mutate(gender = if_else(client_gender == "transge", "other", client_gender)) %>% 
            mutate(closecontactcase = if_else(case_acquisitioninfo == "contact of a confirmed case", 
                                              "yes", NULL)) %>% 
            mutate(disposition = recode(outcome1,
                                        "ill" = "stable", #"ill" value exists in target?
                                        "fatal" = "deceased",
                                        "res. effects" = "recovered")) %>%
            mutate(coviddeath = if_else(outcome1 == "fatal", "yes", NULL)) %>% #check "no" condition
            rename(travel = starts_with("exposure")) %>%
            mutate(across(starts_with("travel"), list(international_loc = parse_location, domestic_loc = parse_province))) %>% #produces travel[1-14]_international/domestic_loc, need to pivot_longer in tables/travel.R
            #need travel flag travel_international and travel_domestic - might be better as derived
            #likely_acquisition 1/2 and case_acquisitioninfo need to be mapped
            mutate(hosp = recode(hospitalized, "unk" = "unknown", "not" = "no")) %>%
            mutate(icu = recode(icu, "unk" = "unknown")) %>% 
            mutate(testingreason = "not collected") %>% 
            mutate(testingreasonspec = "not collected") %>% 
            mutate(indigenousidentity = "not collected") %>%
            mutate(indigenousidentityspec = "not collected") %>%
            mutate(residence_reservecommunitycrownLand = "not collected") %>%
            mutate(occupation = "not collected") %>%
            mutate(rfhypertension = "not collected") %>%
            mutate(rfsicklecelldisease = "not collected") %>%
            mutate(rfcerebrovasculardisease = "not collected") %>%
            mutate(rfpregtrimester = "not collected") %>% 
            mutate(rfvaping = "not collected") %>% 
            mutate(rfpregtrimester = "not collected") %>% 
            mutate(rfotherspec = "not collected") %>%
            mutate(deathcause = "not collected") %>%
            mutate(sequencing = "not collected")
        }
)
