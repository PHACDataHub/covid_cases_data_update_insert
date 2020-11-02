# Newfoundland
pe <- list(
    name = "nl",
    code = "10",
    
    reader = function(path) {
        readxl::read_xlsx(path) %>%
            set_names(str_to_lower(names(.))) %>% # set names to lowercase
            mutate_if(is.POSIXct, as_date) %>% # readxl uses POSIXct objects for dates so we convert to Date objects to match metabase
            mutate_if(is.character, str_to_lower) # make all character cols lower case
    },  
    
    mapper = function(cases_df){
        cases_df %>% 
            rename(sex = gender, #nl reports "sex" but calls it "gender", we add gender = "not collected" below
                   testingreason = test_resason, #sic - "resasos" typo is in linelist submission
                   testingreasonspec = other_test_reason, 
                   classification = case_classification,
                   residency = canadian_resident, 
                   residencecountry =  nonresident_country,
                   detectedatentry = detect_at_point_entry, 
                   dateofentry = point_entry_dt,
                   pointofentrylocation = point_entry_location, 
                   indigenous = identify_indigenous, 
                   indigenousgroup =  indigenous_group,
                   reserve = reside_reserve, 
                   occupation = at_risk_population, #includes HCW and LTC
                   onsetdate = symptom_onset_dt,
                   symcough = cough, 
                   symfever = fever, 
                   symchills =  fever_chills,
                   symsorethroat = sore_throat, 
                   symrunnynose = runny_nose,
                   symrunnynose = runny_nose,	
                   symshortnessofbreath = shortness_breath,	
                   symnausea =	nausea,
                   symheadache = headache,
                   symweakness = general_weakness,	
                   sympain = pain, 
                   symirritability = irritability,
                   symdiarrhea = diarrhea,
                   symother =other_symptoms,
                   symotherspec = other_symptoms_desc,	
                   rfcardiacdisease = cardiac_disease,
                   rfneurodisorder = chronic_neuro, 
                   rfdiabetes = diabetes, 
                   rfimmunodef = immunodeficiency,
                   rfliverdisease = liver_disease, 
                   rfmalignancy = maglignancy, 
                   rfpostpartum = postpartum,
                   rfpregnancy = pregnancy, 
                   rfrenaldisease = renal_disease, 
                   rfrespdisease = repiratory,
                   rfother = other_risks, 
                   rfotherspec =  other_risks_desc, 
                   dxlungausc = abnomral_lung_ausultation,
                   dxmentalstatus = altered_mental_state, 
                   dxpneumonia = pneumonia, 
                   dxcoma = coma,
                   dxconjunctinjection = conjunctival_inject, 
                   dxards = resp_distress_syndrome,
                   dxencephalitis = encephalitis, 
                   dxhypotension = hypotension, 
                   dxpharyngeal = pharyngeal,
                   dxrenalfailure = renal_failure, 
                   dxseizure = seizure, 
                   dxsepsis = sepsis,
                   dxtachypnea = tachypnea, 
                   dxother = other_clinical_eval,
                   dxotherspec = other_clinical_eval_desc,	
                   hosp = hospitalization,	
                   hospstartdate = hospital_start_dt,	
                   hospenddate= hospital_end_dt,
                   icustartdate = icu_start_dt, 	
                   icuenddate =	icu_end_dt, 
                   isolation = isolation,	
                   isolationstartdate = isolation_start_dt, 
                   isolationenddate = isolation_end_dt,
                   mechanicalvent = mechanicalvent, 
                   ventstartdate = mechanicalvent_start_dt,
                   ventenddate = mechanicalvent_end_dt, 
                   disposition = current_status, 	
                   recoverydate = recovery_dt, 	
                   deathresp = death_linked_illness, #deathcovid also?
                   deathcause = cause_of_death, 
                   deathdate = death_dt, 	
                   travel = direct_travel, 	
                   travelfromcountry1 = departure_country_1,
                   traveltocountry1 = destination_country_1, 
                   travelstartdate1	= travel_start_dt_1, 
                   travelenddate1 = travel_end_dt_1,	
                   travelhotel1 = hotel_residence_1, 	
                   travelflight1 = flight_details_1,
                   travelfromcountry2 = departure_country_2,
                   traveltocountry2 = destination_country_2, 
                   travelstartdate2	= travel_start_dt_2, 
                   travelenddate2 = travel_end_dt_2,	
                   travelhotel2 = hotel_residence_2, 	
                   travelflight2 = flight_details_2,		
                   travelfromcountry3 = departure_country_3,
                   traveltocountry3 = destination_country_3, 
                   travelstartdate3	= travel_start_dt_3, 
                   travelenddate3 = travel_end_dt_3,	
                   travelhotel3 = hotel_residence_3, 	
                   travelflight3 = flight_details_3,		
                   travelfromcountry4 = departure_country_4,
                   traveltocountry4 = destination_country_4, 
                   travelstartdate4	= travel_start_dt_4, 
                   travelenddate4 = travel_end_dt_4,	
                   travelhotel4 = hotel_residence_4, 	
                   travelflight4 = flight_details_4,
                   closecontactcase = close_contact_with_case, 
                   closecontactcaseidspt = contact_id_1, #check if used
                   closecontactcasedatefirst = start_contact_dt_1, #check if used
                   closecontactcasedate = end_contact_dt_1, #check if used
                   closecontactcasesetting = contact_setting_1, 
                   closecontactcasesettingspec = contact_setting_other_1,
                   closecontactcaseid2 = contact_id_2, 
                   closecontactcasedatefirst2 = start_contact_dt_2, 
                   closecontactcasedate2 = end_contact_dt_2, 	
                   closecontactcasesetting2 = contact_setting_2,  
                   closecontactcasesettingspec2 = contact_setting_other_2,
                   closecontactcaseid3 = contact_id_3,	
                   closecontactother =contact_with_traveler, 
                   animalcontact = animal_contact,
                   animalcontactspec = animal_notes, 
                   animalcontactsettingcity = animal_contact_city, 
                   healthfacilityexposure = healthcare_facility_visit, 
                   comments = travel_comments_1,
                   labid1 = phml_specimen_id, 	
                   labspecimentype1 = type_source, 
                   labspecimencollectiondate1 = specimen_collection_dt,
                   labtestmethod1	= test_method, 
                   labtestresult1 = phml_result, 	
                   labtestresultdate1 = phml_test_dt,
                   nmlconfirmation = nml_result, 	
                   nmlconfirmationdate = nml_test_dt, 
                   numberofcontacts = contact_count)  %>%
            mutate(pt = "nl") %>% 
            mutate(classification = recode(classification,
                                           "confirmed-lab" = "confirmed",
                                           "confirmed-epi-link" = "probable")) %>% 
            mutate(occupationspec = if_else(occupation == "ltc worker", "ltc worker", NULL)) %>% #map to current values
            mutate(occupation = recode(occupation ,
                                       "healthcare worker with direct patient contact" = "healthcare worker",
                                       "healthcare worker with no direct patient contact" = "healthcare worker" ,
                                       "school/daycare worker/attendee" = "school or daycare worker/attendee",
                                       "lab worker handling biological specimens" = "laboratory worker",
                                       "ltc worker" = "healthcare worker",
                                       "vet/animal worker" = "veterinary worker",
                                       "pch/cch/home support worker" = "healthcare worker",
                                       "resident of ltc" = "long-term care resident")) %>% #map to current values 
            mutate(ageunit = if_else(age_years == 0, "months", "years"))  %>%
            mutate(age = if_else(ageunit == "months", age_months, age_years)) %>% 
            mutate(asymptomatic = recode(initial_symptomatic_status,
                                         "asymptomatic" = "yes",
                                         "symptomatic" = "no" ,
            )) %>%
            
            mutate(rfother = if_else(smoking == "yes", "smoking", rfother)) %>% 
            mutate(animalcontactsetting = if_else(animal_contact_home == "yes", "home", NULL))%>% 
            mutate(animalcontactsetting = if_else(animal_contact_work == "yes", "work", animalcontactsetting)) %>% 
            mutate( animalcontactsetting = if_else(animal_contact_travel == "yes", "during travel", animalcontactsetting)) %>% 
            mutate(animalcontactsetting = if_else(animal_contact_market == "yes", "live animal market", animalcontactsetting)) %>% 
            rename(travel1 = matches("departure_country_\\d{1,2}")) %>% 
            rename(travel2 = matches("destination_country_\\d{1,2}")) %>% 
            mutate(across(matches("travel\\d{2,3}"), list(international_loc = parse_location, domestic_loc = parse_province))) %>% 
            mutate(gender = "not collected")
    }
)
