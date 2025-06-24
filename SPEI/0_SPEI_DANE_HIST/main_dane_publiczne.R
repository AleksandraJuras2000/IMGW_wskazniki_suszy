###############################################################################################################
###############################################################################################################
#############################     DANE SYNOPTYCZNE       ######################################################
###############################################################################################################
###############################################################################################################
#
#
#
#
#################################################################################################################
###################     POBIERANIE I CZYSZCZENIE DANYCH DOBOWYCH     ############################################
#################################################################################################################



synop_daily <- meteo_imgw_daily(rank = "synop", year = 1971:2000, col_names = "polish", status = TRUE, allow_failure = FALSE)

synop_daily_clean <- clean_data_daily(synop_daily, rank="synop")




#################################################################################################################
######################    POBIERANIE I CZYSZCZENIE DANYCH TERMINOWYCH    ########################################
#################################################################################################################



synop_hourly <- meteo_imgw_hourly(rank = "synop", year = 1971:2000, col_names = "polish",
                                   allow_failure = FALSE, status = TRUE)


synop_hourly_clean <- clean_data_hourly(synop_hourly, rank="synop")


#################################################################################################################
#############     RĘCZNE SPRAWDZANIE DANYCH BO NIE MA INNEJ OPCJI.. CHAOS    ####################################
#################################################################################################################


#sprawdzenie czy mamy koplety pomiarow dla stacji
agrr1 <- synop_daily_clean %>% group_by(id) %>% summarise(name= first(`Nazwa stacji`), record_count = n())
agrr2 <- synop_hourly_clean %>% group_by(id) %>% summarise(name= first(`Nazwa stacji`), record_count = n())


#w aggr1 mamy 52 stacji, natomiast w aggr2 55 stacji:
# 1) Poznań-Ławica... w danych_daily jest Poznań
# 2) Warszawa-Okęcie.. w daily jest Warszawa
# 3) Kołobrzeg-Dźwirzyno w daily jest Kołobrzeg 
# 4) Wroclaw-Starachowice w daily jest Wroclaw
# 5) Katowice-Muchowiec w hourly jest Katowice
# i dlatego nam to odfiltrowało i tutaj pytanie czy to sa te same stacje tylko inna nazwa? 
# narazie zostawiam je odfiltrowane bo w pozniejszych latach moze sie cos wykrzaczyc


#################################################################################################################
###########     ZLACZENIE, DODANIE SREDNIEJ I DODAWANIE SZEROKOSCI GEOGRAFICZNEJ          #######################
#################################################################################################################


synop_full_data <- join_data_and_add_coors(synop_daily_clean, synop_hourly_clean, rank="synop")


#################################################################################################################
#######################    DODAWANIE ETO I RA   #################################################################
#################################################################################################################


ra <- read.csv('ra.csv')
synop_full_data$Ra <- ra$Ra.Mjm.2day.1.[match(synop_full_data$szer_mies_dzien, ra$Szer_mies_dzien)]
synop_full_data$Eto <- 0.408*0.001*(synop_full_data$`Srednia temperatura dobowa [°C]`+17)*(synop_full_data$`Maksymalna temperatura dobowa [°C]`- synop_full_data$`Minimalna temperatura dobowa [°C]`)^0.724*synop_full_data$Ra


###############################################################################################################
###############################################################################################################
################################  DANE KLIMATYCZNE    #########################################################
###############################################################################################################
###############################################################################################################
#
#
#
#################################################################################################################
###################     POBIERANIE I CZYSZCZENIE DANYCH DOBOWYCH     ############################################
#################################################################################################################



climate_daily <- meteo_imgw_daily(rank = "climate", year = 1971:2000, col_names = "polish", status = TRUE, allow_failure = FALSE)

climate_daily_clean <- clean_data_daily(climate_daily, rank="climate")


#################################################################################################################
######################    POBIERANIE I CZYSZCZENIE DANYCH TERMINOWYCH    ########################################
#################################################################################################################



climate_hourly <- meteo_imgw_hourly(rank = "climate", year = 1971:2000, coords = FALSE, col_names = "polish",
                                  allow_failure = FALSE, status = TRUE)


climate_hourly_clean <- clean_data_hourly(climate_hourly, rank="climate")



#################################################################################################################
#############     RĘCZNE SPRAWDZANIE DANYCH BO NIE MA INNEJ OPCJI.. CHAOS    ####################################
#################################################################################################################


#sprawdzenie czy mamy koplety pomiarow dla stacji
agrr1 <- climate_daily_clean %>% group_by(id) %>% summarise(name= first(`Nazwa stacji`), record_count = n())
agrr2 <- climate_hourly_clean %>% group_by(id) %>% summarise(name= first(`Nazwa stacji`), record_count = n())


#################################################################################################################
###########     ZLACZENIE, DODANIE SREDNIEJ I DODAWANIE SZEROKOSCI GEOGRAFICZNEJ          #######################
#################################################################################################################


climate_full_data <- join_data_and_add_coors(climate_daily_clean, climate_hourly_clean, rank="climate")



#################################################################################################################
#######################    DODAWANIE ETO I RA   #################################################################
#################################################################################################################


climate_full_data$Ra <- ra$Ra.Mjm.2day.1.[match(climate_full_data$szer_mies_dzien, ra$Szer_mies_dzien)]
climate_full_data$Eto <- 0.408*0.001*(climate_full_data$`Srednia temperatura dobowa [°C]`+17)*(climate_full_data$`Maksymalna temperatura dobowa [°C]`- climate_full_data$`Minimalna temperatura dobowa [°C]`)^0.724*climate_full_data$Ra


###################################################################################################################
###################################################################################################################
##########################  CZYSZCZENIE Z MIAST(DANE < 15%)  ######################################################
###################################################################################################################
###################################################################################################################

#  OPCJONALNIE najpier zobaczmy ile jest brakujących danych w poszczegółnych latach dla każdej ze stacji__

count_NA_per_year(synop_full_data, "synop")
count_NA_per_year(climate_full_data, "klimat")



odfiltrowane_dane_synop <- odfiltruj_dane_85_procent(synop_full_data)
odfiltrowane_dane_climat <- odfiltruj_dane_85_procent(climate_full_data)




#  OPCJONALNIE zobaczmy teraz co nam odfiltrowało

count_NA_per_year(odfiltrowane_dane_synop, "synop")
count_NA_per_year(odfiltrowane_dane_climat, "klimat")


###################################################################################################################
#######################    ŁĄCZYMY DANE SYNOP I KLIMAT     ########################################################
###################################################################################################################

#odfiltrowane_dane_climat <- odfiltrowane_dane_climat %>% rename(id = `Kod stacji`)

odfiltrowane_dane_full <- bind_rows(odfiltrowane_dane_synop, odfiltrowane_dane_climat)


###################################################################################################################
#################       PREPROCESSING POŁĄCZONYCH DANYCH      #####################################################
###################################################################################################################


# Zmiana nazwy kolumny 'stara_nazwa' na 'nowa_nazwa'
colnames(odfiltrowane_dane_full)[colnames(odfiltrowane_dane_full) == "Maksymalna temperatura dobowa [°C]"] <- "Max_temp"
colnames(odfiltrowane_dane_full)[colnames(odfiltrowane_dane_full) == "Minimalna temperatura dobowa [°C]"] <- "Min_temp"
odfiltrowane_dane_full$data <- as.Date(with(odfiltrowane_dane_full, paste(Rok, Miesiac, Dzien, sep = "-")), format = "%Y-%m-%d")


#dodanie jedynych brakujących wspołrzednych dla GDAŃSK-RĘBIECHOWO
#wczytujemy plik stacje
stacje <- read.csv("tabela_stacje.csv", encoding = "UTF-8")
stacje <- stacje[, c(5,6,8,9, 37)]

# łączymy dwie tabele aby w jednej tabeli były stacje i ich pomiary oraz współrzędne
odfiltrowane_dane_full <- merge(odfiltrowane_dane_full, stacje, by.x = "Nazwa stacji", by.y = "Nazwa_st", all.x = TRUE)
odfiltrowane_dane_full <- odfiltrowane_dane_full[, c(2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 12, 13, 14, 19, 20)]


###################################################################################################################
#################     UZUPEŁNIANIE DANYCH (METODA WYTŁUMACZONA PRZY FUNKCJACH)     ################################
###################################################################################################################

# WYŚWIELAMY ILE JEST BRAKUJĄCYCH DANYCH
View(filter(odfiltrowane_dane_full, is.na(Eto)))

#zapisujemy indeksy wierszy w których brakuje danych temperaturowych
indeksy_temp <- which(is.na(odfiltrowane_dane_full$Eto))

#funckja uzupelniajaca braki
final <- fill_the_gap(odfiltrowane_dane_full)  #<---- FINAŁ!!!!!!

final$szer_mies_dzien <- NULL

#ile razy dana stacja wystepuje w danym okresie czasowym
agrr_full <- final %>% group_by(id) %>% summarise(name= first(`Nazwa stacji`), record_count = n())

#wyswietlamy uzupełnione wiersze 
View(final[indeksy_temp,])

#zapisujemy do csv
write.csv(final, "dane_SPEI_uzupelnione_1971_2000.csv")


