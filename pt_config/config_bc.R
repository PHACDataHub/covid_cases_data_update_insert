# BC
bc <- list(
    name = "bc",
    code = "59",
    
    reader = function(path) {
        read_csv(path, guess_max = 100000) %>%
            set_names(str_to_lower(names(.))) %>% # set column names to lower case
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },
    
    mapper = function(cases_df){ 
        cases_df %>% 
            rename(ptcaseid = unique_id,
                   reporteddate = reported_date,
                   age = age_combined,
                   rfpregnancy = pregnant,
                   hosp = ever_hospitalized,
                   icu = ever_icu,
                   closecontactcase = contact_with_ncov_case,
                   exposurecat = exposure,
                   travel_international = travel_outside_canada,
                   travel_domestic = travel_within_canada,
                   deceased = died
                   ) %>%  
            mutate(ageunit = "years") %>% 
            mutate(pt = "bc") %>%
            mutate(classification = recode(case_classification, 
                                           "probable: lab" = "probable",
                                           "probable: epi-linked" = "probable")) %>% 
            mutate(onsetdate = as.Date(symptom_onset_date, origin = "1899-12-30")) %>% 
            mutate(disposition = recode(status_derived, 
                                        "removed from isolation" = "recovered",
                                        "died" = "deceased",
                                        "lost to follow up" = "unknown",
                                        "active" = "ill")) %>% 
            mutate(exposuresetting = recode(reportable_ob_setting, 
                                            "long term care facility" = "longterm care facility",
                                            "workplace not otherwise specified" = "office", #?
                                            "acute care facility" = "healthcare setting – acute care facility",
                                            "independent living" = "retirement residence",
                                            "conference" = "mass gathering event",
                                            "assisted living" = "retirement residence", #? "longterm care facility"
                                            "other residential facility type" = "longterm care facility", #?
                                            "shelter" = "congregate living setting",                   
                                            "workplace/communal living" = "congregate living setting", #?
                                            "group home (community living)" = "congregate living setting",
                                            "religious institution" = "recreational facility")) %>% 
            mutate(closecontactcase = if_else(closecontactcase == "yes", "yes", 
                                              if_else(cluster == "yes", "yes", 
                                                      if_else(unknown_source == "yes", "no", NULL)))) %>%
            mutate(travel_international_loc = parse_location(travel_outside_canada_specify, type = "country")) %>% 
            mutate(travel_domestic_loc = parse_location(travel_within_canada_province, type = "province")) %>% 
            mutate(testingreason = "not collected") %>% 
            mutate(testingreasonspec = "not collected") %>% 
            mutate(residencecountry = "not collected") %>%
            mutate(gender = "not collected") %>%  
            mutate(healthregion = "not collected") %>%
            mutate(fsa = "not collected") %>%
            mutate(dwellingtype = "not collected") %>%
            mutate(race = "not collected") %>%
            mutate(racespec = "not collected") %>%
            mutate(indigenousidentity = "not collected") %>%
            mutate(indigenousidentityspec = "not collected") %>%
            mutate(residence_reservecommunitycrownland = "not collected") %>%
            mutate(occupationhcw = "not collected") %>%
            mutate(occupationhcwrole = "not collected") %>%
            mutate(occupationhcwroleSpec = "not collected") %>%
            mutate(occupationpcwpatientcare = "not collected") %>%
            mutate(occupation = "not collected") %>%
            mutate(occupationspec = "not collected") %>%
            mutate(rotationalworker = "not collected") %>%
            mutate(tempforeignworker = "not collected") %>%
            mutate(asymptomatic = "not collected") %>%
            mutate(rfhypertension = "not collected") %>%
            mutate(rfcardiovasculardisease = "not collected") %>% 
            mutate(rfdiabetes = "not collected") %>%
            mutate(rfchronickidneydisease = "not collected") %>%
            mutate(rfcopd = "not collected") %>%
            mutate(rfasthma = "not collected") %>%
            mutate(rfliverdisease = "not collected") %>%
            mutate(rfmalignancy = "not collected") %>%
            mutate(rfsicklecelldisease = "not collected") %>%
            mutate(rfimmunodef = "not collected") %>%
            mutate(rfneurodisorder = "not collected") %>%
            mutate(rfcerebrovasculardisease = "not collected") %>%
            #mutate(rfpregnancy = "not collected") %>% # currently provided but listed as not collected by province
            mutate(rfpregtrimester = "not collected") %>%
            mutate(rfpostpartum = "not collected") %>%
            mutate(rfobesity = "not collected") %>%
            mutate(rfsmokingtobacco = "not collected") %>%
            mutate(rfvaping = "not collected") %>%
            mutate(rfproblematicsubstanceuse = "not collected") %>%
            mutate(rfother = "not collected") %>%
            mutate(rfotherspec = "not collected") %>%
            mutate(deathresp = "not collected") %>%
            mutate(deathcause = "not collected") %>%
            mutate(closecontactother = "not collected") %>%
            mutate(closecontactotherspec = "not collected") %>%
            mutate(numberofcontacts = "not collected") %>%
            mutate(exposuresetting_otherspec = "not collected") %>%
            mutate(exposuresettingyype = "not collected") %>%
            mutate(exposuresettingtype_otherspec = "not collected")
    })


