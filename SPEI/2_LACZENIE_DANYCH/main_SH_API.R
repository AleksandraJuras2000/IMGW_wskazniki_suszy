library(dplyr)
library(openxlsx)
library(writexl)
library(lubridate)
library(SPEI)
library(readxl)



dane_2025 <- read.csv("../1_SCIAGANIE_NOWYCH_DANYCH/DANE/dane_spei_2025.01_2025.05.csv", fileEncoding = "UTF-8-BOM") 
#<-------- pamiętać aby zmienic nazwe pliku z nowymi danymi ściągniętymi z sh api 


################ czyszczenie i uzupelnianie brakujacyuch dat ################


dane_2025_uzupelnione <- clean_data_from_SH_API(dane_2025)


############### dodanie szerokosci geog ########################################


dane_2025_uzupelnione_z_szer <- add_coors_from_SH_API(dane_2025_uzupelnione)


############## dodanie ETO ###########################################

ra <- read.csv('ra.csv')
dane_2025_uzupelnione_z_szer$Ra <- ra$Ra.Mjm.2day.1.[match(dane_2025_uzupelnione_z_szer$szer_mies_dzien, ra$Szer_mies_dzien)]
dane_2025_uzupelnione_z_szer$Eto <- 0.408*0.001*(dane_2025_uzupelnione_z_szer$mean+17)*(dane_2025_uzupelnione_z_szer$max- dane_2025_uzupelnione_z_szer$min)^0.724*dane_2025_uzupelnione_z_szer$Ra


################### uzupelniamy luki w danych ########################################

count_NA_per_year_SH_API(dane_2025_uzupelnione_z_szer) #ile stacji ma brakujące eto

stacje <- read.csv("tabela_stacje.csv", encoding = "UTF-8")
stacje <- stacje[, c(5,6,8,9, 37)]
# łączymy dwie tabele aby w jednej tabeli były stacje i ich pomiary oraz współrzędne
dane_2025_uzupelnione_ze_wspol <- merge(dane_2025_uzupelnione_z_szer, stacje, by.x = "Nazwa.stacji", by.y = "Nazwa_st", all.x = TRUE)
dane_2025_uzupelnione_ze_wspol <- dane_2025_uzupelnione_ze_wspol[, !names(dane_2025_uzupelnione_ze_wspol) %in% c("RODZ", "ppKODP")]

################### uzupelniamy luki w danych ########################################
dane_2025_final <- fill_the_gap_SH_API(dane_2025_uzupelnione_ze_wspol)

##################### sortowanie ##########################################################
dane_2025_final_sorted <- dane_2025_final[order(dane_2025_final$data, decreasing = FALSE), ]




################### ŁĄCZYMY DANE ##########################################################################

# to jest potrzebne tylko na pierwszy raz potem nazwy kolumn sie zmieniaja i trzeba bedzie albo pokukladac je inaczej zeby sie skleily albo je inaczej ponazwyac
#wczytujemy dane stare 
stare_dane <- read.csv("dane_tempe/Dane_do_SPEI_1971_2025_05.csv") #<------------------- zmienic sciezke


#data as data
stare_dane$Data <- as.Date(stare_dane$Data)


######### Zmień nazwy kolumn nowych danych ##########################

names(dane_2025_final_sorted) <- gsub("mean", "Srednia", names(dane_2025_final_sorted))
names(dane_2025_final_sorted) <- gsub("data", "Data", names(dane_2025_final_sorted))
nowe_dane <- dane_2025_final_sorted[, c(2, 1, 4, 20, 10, 11, 12, 6, 5, 8, 9, 7, 16, 17, 13, 14, 15, 18, 19)]
nowe_dane$Data <- as.Date(nowe_dane$Data)


#sklejamy stare z nowymi
sklejone_dane <- bind_rows(stare_dane, nowe_dane)


#zapisujemy 
write.csv(sklejone_dane, "dane_wynikowe/dane_do_SPEI_1971_2025_05.csv") #<------- to bedą nasze "przyszle" stare_dane





################## ŁĄCZENIE Z OPADAMI ######################################################


#zaczytujemy dane z policzonym ETO
dane <- sklejone_dane


####szybkie statystyki

# agregacja po id (czyli ile stacji jest kompletnych a ile nie)
agrr <- dane %>% group_by(id) %>% summarise(record_count = n())


#robimy miesieczne sumy eto

eto_mies <- dane %>%
  group_by(id, Nazwa.stacji, rank_code, Rok, Miesiac, SZER_G, DLUG_G) %>%
  summarise(Suma_eto = sum(Eto, na.rm = TRUE),
            .groups = 'drop') %>%
  arrange(id, Rok, Miesiac)

## znowu agregacja po id
agrr_id_mies <- eto_mies %>% group_by(id) %>% summarise(record_count = n())




#zaczytujemy opady
opad_mies <- read.csv("dane_opad_(przklejone_z_spi)/opad_mies_1971_202505.csv")
opad_mies <- opad_mies[, !grepl("X", names(opad_mies))]


#zmiana typu danych i dodanie dwoch kolumn
opad_mies <- opad_mies %>%
  mutate(
    aggregation_date = as.Date(aggregation_date),
    Rok = as.integer(year(aggregation_date)),
    Miesiac = as.integer(month(aggregation_date))
  )



#łaczenie
polaczona_tabela <- eto_mies %>%
  left_join(
    opad_mies %>% 
      select(dre.name, Rok, Miesiac, opad = value, NAZWA_ST),
    by = c("id" = "dre.name", "Rok" = "Rok", "Miesiac" = "Miesiac")
  )




full_data <- filter(polaczona_tabela, !is.na(opad))
full_data$bilans_wodny <- full_data$opad - full_data$Suma_eto


full_data <- full_data[, c(3, 1, 2, 4, 5, 8, 9, 11)]

write.csv(full_data, "../DANE/dane_do_spei_1971_202505.csv")


## znowu agregacja po id
agrr <- full_data %>% group_by(id) %>% summarise(record_count = n())




########################################### licznenie SPEI ###################################################


dane_odfiltr <- full_data %>% filter(id %in% agrr$id[agrr$record_count == max(agrr$record_count)])



library(SPEI)
library(dplyr)
library(lubridate)

# 1. Przygotowanie danych - obliczenie bilansu wodnego (opad - ETO)
tabela_polaczona_final <- dane_odfiltr %>%
  mutate(
    bilans_wodny = ifelse(bilans_wodny == 0, 0.001, bilans_wodny),  # Zamiana 0 na 0.001
  )


tabela_polaczona_final <- tabela_polaczona_final %>% mutate(Data = as.Date(paste(Rok, Miesiac, "01", sep = "-")))




# 2. Okres referencyjny 
ref_start <- c(1981, 1)  # Początek okresu referencyjnego (rok, miesiąc)
ref_end <- c(2020, 12)   # Koniec okresu referencyjnego

# 3. Obliczanie SPEI dla różnych skal czasowych
spei_result <- tabela_polaczona_final %>%
  group_by(id) %>%
  group_modify(~{
    start_year <- year(min(.x$Data))
    start_month <- month(min(.x$Data))
    
    # Tworzymy szereg czasowy bilansu wodnego
    ts_data <- ts(.x$bilans_wodny, 
                  start = c(start_year, start_month), 
                  frequency = 12)
    
    # Obliczamy SPEI dla różnych skal
    .x$SPEI1 <- as.numeric(spei(ts_data, scale = 1, ref.start = ref_start, ref.end = ref_end, 
                                distribution = 'log-Logistic')$fitted)
    .x$SPEI3 <- as.numeric(spei(ts_data, scale = 3, ref.start = ref_start, ref.end = ref_end,
                                distribution = 'log-Logistic')$fitted)
    .x$SPEI6 <- as.numeric(spei(ts_data, scale = 6, ref.start = ref_start, ref.end = ref_end,
                                distribution = 'log-Logistic')$fitted)
    .x$SPEI12 <- as.numeric(spei(ts_data, scale = 12, ref.start = ref_start, ref.end = ref_end,
                                 distribution = 'log-Logistic')$fitted)
    .x$SPEI24 <- as.numeric(spei(ts_data, scale = 24, ref.start = ref_start, ref.end = ref_end,
                                 distribution = 'log-Logistic')$fitted)
    
    return(.x)
  }) %>%
  ungroup()



write.csv(spei_result, "../DANE/SPEI_1971_2025.05_(1,3,6,12,24).csv")






