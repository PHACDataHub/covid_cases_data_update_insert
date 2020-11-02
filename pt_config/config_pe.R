# PEI
pe <- list(
    name = "pe",
    code = "11",
    
    reader = function(path) {
        readxl::read_xlsx(path) %>%
            set_names(str_to_lower(names(.))) %>% # set names to lowercase
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },  
    
    mapper = function(cases_df){
        cases_df %>% 
            mutate(pt = "pe") %>%
            rename(travel1 = matches("travelto")) %>% 
            rename(travel2 = matches("travelfrom")) %>% 
            mutate(across(matches("travel[0-9]{1}"), list(international_loc = parse_location, domestic_loc = parse_province))) %>% 
            mutate(fsa = "not collected")
        }
    )
