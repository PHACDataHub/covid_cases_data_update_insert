# Quebec
qc <- list(
    name = "qc",
    code = "24",
    
    reader = function(path) {
        read_delim(path, delim = ";", locale = locale(encoding = "UTF-8"), na = ".", guess_max = 100000) %>% 
            set_names(str_to_lower(names(.))) %>% 
                                         #, from = 'UTF-8', to = 'ASCII//TRANSLIT'))) #%>% 
            mutate_if(is.character, str_to_lower)
    },
    
    mapper = function(cases_df){ 
        cases_df %>%
            rename(ptcaseid = `identifiant de la personne (tsp) infocentre`, 
                   healthregion = `rss de résidence - infocentre (tsp)`,
                   onsetdate = `date de début des symptômes (tsp)`,
                   age = `âge calculé à la date de déclaration - infocentre (tsp)` #some values >120, 1 negative (-1)
                   #agegrouping = `groupe d'age calcule a la date de declaration - infocentre (tsp)` #we derive this instead
            ) %>% 
            mutate(pt = "qc") %>% 
            mutate(ageunit = "years") %>% 
            mutate(reporteddate = as.Date(`date de déclaration - infocentre (tsp)`, "%Y-%m-%d")) %>% #not sure a.Date is needed?
            mutate(labspecimencollectiondate = as.Date(`date de prélèvement (laboratoire)`, "%Y-%m-%d")) %>% 
            mutate(hospstartdate1 = as.Date(`date d'admission 1 hospitalisation covid-19 (med-écho)`, "%Y-%m-%d")) %>%
            mutate(hospenddate1 = as.Date(`date de sortie 1 hospitalisation covid-19 (med-écho)`, "%Y-%m-%d")) %>%
            mutate(hospstartdate2 = as.Date(`date d'admission 2 hospitalisation covid-19 (med-écho)`, "%Y-%m-%d")) %>%
            mutate(hospenddate2 = as.Date(`date de sortie 2 hospitalisation covid-19 (med-écho)`, "%Y-%m-%d")) %>%
            mutate(hospstartdate3 = as.Date(`date d'admission 3 hospitalisation covid-19 (med-écho)`, "%Y-%m-%d")) %>%
            mutate(hospenddate3 = as.Date(`date de sortie 3 hospitalisation covid-19 (med-écho)`, "%Y-%m-%d")) %>%
            mutate(deathdate = as.Date(`date du décès (tsp)`, "%Y-%m-%d")) %>%
            mutate(disposition = recode(`évolution du cas - infocentre (tsp)`,
                                        "rétabli" = "recovered",
                                        "décédé" = "deceased",
                                        "épisode en cours" = "ill",
            )) %>% #count(disposition)
            mutate(classification = recode(`type de personne`,
                                           "cas confirmé par lien épidémiologique" = "probable",
                                           "cas confirmé par laboratoire" = "confirmed"
            )) %>% #count(classification)
            mutate(travel_domestic = recode(`cas hors québec (tsp)`,
                                            "0-non" = "no", 
                                            "1-oui" = "yes")) %>% #count(travel_domestic)
            mutate(closecontactcase = recode(`contact avec un cas connu (tsp)`,
                                             "1-oui" = "yes")) %>% #count(closecontactcase)
            mutate(sex = recode(`sexe (tsp)`,
                                   "féminin" = "female",
                                   "masculin" = "male",
                                   "inconnu" = "unknown"
            )) %>% #count(sex)
            mutate(labtestresult = recode(`résultat du laboratoire (tsp)`,
                                         "positif" = "positive",
                                         "équivoque" = "inconclusive" 
            )) %>% #count(labtestresult)
            mutate(asymptomatic = recode(`asymptomatique (tsp)`,
                                         "0-non" = "no", 
                                         "1-oui" = "yes")) %>% #count(asymptomatic)
            mutate(pregnancy = recode(`grossesse (tsp)`,
                                      "oui" = "yes",
                                      "non" = "no",
                                      "inconnu" = "unknown")) %>% #count(pregnancy)
            mutate(postpartum = recode(`post-partum <= 6 semaines (tsp)`,
                                          "oui" = "yes",
                                          "non" = "no",
                                          "inconnu" = "unknown")) %>% #count(postpartum)   #will need to align RF vars in DISCOVER RF_lookup table
            mutate(smokingtobacco = recode(`tabagisme (tsp)`,
                                          "oui" = "yes",
                                          "non" = "no",
                                          "inconnu" = "unknown")) %>% #count(smokingtobacco)  
            mutate(indigenous = recode(`autochtone (tsp)`,
                                         "non" = "no",
                                         "oui" = "yes",
                                         "inconnu" = "unknown"
            )) %>% 
            mutate(occupationhcw = recode(`travailleur de la santé (tsp)`,
                                         "0-non" = "no",
                                         "1-oui" = "yes"
            )) %>%  #count(occupationhcw)   
            mutate(recoverydate = if_else(disposition == "recovered", `date de l'évaluation de l'état - infocentre (tsp)`, NULL)) %>% #count(disposition)   
            mutate(deathdate = if_else(is.na(deathdate) & disposition == "deceased", `date de l'évaluation de l'état - infocentre (tsp)`, deathdate)) %>% #count(deathdate) 
            mutate(icu = recode(`admission aux soins intensifs (med-écho)`,
                                "non" = "no",
                                "oui" = "yes")) #%>% count(icu)
            
            # mutate(indigenousidentityspec = "not collected") %>%
            # mutate(gender = "not collected") %>% 
            # mutate(dwellingtype = "not collected") %>% 
            # mutate(racespec = "not collected") %>%
            # mutate(occupationhcwpatientcare = "not collected") %>% 
            # mutate(rfsicklecelldisease = "not collected") %>% 
            # mutate(rfvaping = "not collected") %>% 
            # mutate(rfproblematicsubstanceUse = "not collected") %>% 
            # mutate(closecontacttravelspec = "not collected")
            
    })
