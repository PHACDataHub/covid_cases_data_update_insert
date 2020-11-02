# Manitoba
mb <- list(
    name = "mb",
    code = "46",
    
    reader = function(path) {
        # labspecimencollectiondate and labtestresultdate are unix timestamps so we read them as text and convert them to date later in the mapper
        # because read_xlsx can't deal with properly
        col_types <- c("text","text","date","text","text","text","text","numeric","text","text",
                       "date","text","text","text","text","text","text","text","text","text",
                       "text","text","text","text","text","text","text","text","text","text",
                       "text","text","text","date","date","text","date","date","text","date",
                       "date","text","date","date","date","text","date","text","text","text",
                       "text","text","text","text","text","text","text","text","text","text",
                       "text","text","text","text","text","text","text","text","text","text",
                       "text","text","text","text","text","text","text","text","text","text",
                       "text","text","text","text","text","text","text","text","text","text")
        readxl::read_xlsx(path, col_types = col_types) %>%
            set_names(str_to_lower(names(.))) %>% # set names to lowercase
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },  
    
    mapper = function(cases_df){
        cases_df %>% 
            # convert labspecimencollectiondate and labtestresultdate from unix timestamp to date
            mutate_at(vars(starts_with("labspecimencollectiondate"), starts_with("labtestresultdate")), ~as_date(as_datetime(as.integer(.), origin = "1960-01-01"))) %>%
            mutate(classification = recode(classification, "lab confirmed" = "confirmed")) %>%
            mutate(pt = "mb") %>% 
            rename(travel_international_loc = matches("traveltocountry")) %>% #rename first
            mutate(across(matches("travel_international_loc"), parse_location)) %>%  #then clean up
            mutate(racespec = "not collected") %>% 
            mutate(indigenousidentityspec = "not collected") %>% 
            mutate(residence_reservecommunitycrownland = "not collected") %>% 
            mutate(occupationspec = "not collected")
    }
)
