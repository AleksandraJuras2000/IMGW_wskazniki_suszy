library(climate)
library(dplyr)
library(lubridate)
library(tidyr)
library(XML)
library(sp)
library(writexl)
library(readxl)
library(openxlsx)
library(gstat)
library(data.table)
##############################################################################################

clean_data_daily = function(x, rank) {

  
 # usuwa statusy
  cols_to_remove <- grep("^Status", colnames(x))
  cols_to_keep <- grep("^Status pomiaru (TMAX|TMIN)$", colnames(x))
  cols_to_remove <- setdiff(cols_to_remove, cols_to_keep)
  x[, cols_to_remove] <- NULL
  
  if ("Kod stacji" %in% names(x)) {
    names(x)[names(x) == "Kod stacji"] <- "id"
  }
  
  
  x$`Nazwa stacji` <- trimws(x$`Nazwa stacji`)

  
  if(rank == "synop"){
    
    
    #DODATKOWA SEKCJA ZAMIENIANIA NAZW POZNAN NA POZNAN ŁAWICA i RESZTA
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "POZNAŃ" = "POZNAŃ-ŁAWICA"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "WARSZAWA" = "WARSZAWA-OKĘCIE"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "WROCŁAW" = "WROCŁAW-STRACHOWICE"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "KOŁOBRZEG" = "KOŁOBRZEG-DŹWIRZYNO"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "KATOWICE" = "KATOWICE-MUCHOWIEC"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "ŁÓDŹ" = "ŁÓDŹ-LUBLINEK"))
    
    
    
    #zostawilismy tylko dwa ststusy i tutaj te ktore nie maja NA zostaja odfiltrowane
    x <- x[is.na(x$`Status pomiaru TMIN`), ] 
    x <- x[is.na(x$`Status pomiaru TMAX`), ] 
    
    #x <- x[, c(1, 2, 5, 6, 7, 8, 9, 11, 3, 4)]
    x <- x[, c(1, 2, 6, 3, 4, 5, 7, 9, 11)]

    stacje <- c("KOŁOBRZEG-DŹWIRZYNO", "KOSZALIN", "USTKA", "ŁEBA", "HEL", "GDAŃSK-RĘBIECHOWO", 
                "GDAŃSK-ŚWIBNO", "ELBLĄG-MILEJEWO", "KĘTRZYN", "SUWAŁKI", "ŚWINOUJŚCIE", "SZCZECIN", 
                "RESKO-SMÓLSKO", "SZCZECINEK", "PIŁA", "CHOJNICE", "TORUŃ", "MŁAWA", "OLSZTYN", 
                "MIKOŁAJKI","OSTROŁĘKA", "BIAŁYSTOK", "GORZÓW WIELKOPOLSKI", "SŁUBICE", "POZNAŃ-ŁAWICA", 
                "KOŁO", "PŁOCK", "WARSZAWA-OKĘCIE", "SIEDLCE", "TERESPOL", "ZIELONA GÓRA", "LEGNICA", 
                "LESZNO", "WROCŁAW-STRACHOWICE", "KALISZ", "WIELUŃ", "ŁÓDŹ-LUBLINEK", "SULEJÓW", "KOZIENICE", 
                "LUBLIN-RADAWIEC", "WŁODAWA", "JELENIA GÓRA", "ŚNIEŻKA", "KŁODZKO", "OPOLE", "RACIBÓRZ", 
                "CZĘSTOCHOWA", "KATOWICE-MUCHOWIEC", "KRAKÓW-BALICE", "KIELCE-SUKÓW", "TARNÓW", 
                "RZESZÓW-JASIONKA", "SANDOMIERZ", "ZAMOŚĆ", "BIELSKO-BIAŁA", "ZAKOPANE", "KASPROWY WIERCH", 
                "NOWY SĄCZ", "KROSNO", "LESKO", "PRZEMYŚL")
    
    x <- x[x$`Nazwa stacji` %in% stacje, ]
    
    
    x$data_czas <- make_datetime(year = x$Rok, month = x$Miesiac, day = x$Dzien)   #dodanie kolumny w formacie data
    start_date <- min(x$data)  # początek okresu
    end_date <- max(x$data)    # koniec okresu
    all_dates <- seq(from = start_date, to = end_date, by = "day")
    
    full_data <- expand.grid(
      `id` = unique(x$`id`),data_czas = all_dates) %>%
      left_join(x %>% select(`id`, rank_code = rank_code, `Nazwa stacji` = `Nazwa stacji`) %>%
                  distinct(),
                by = "id")
    
    full_data <- left_join(full_data, x, by = c("id", "data_czas"))
    full_data <- full_data[, -c(5, 6)] 
    colnames(full_data) <- gsub("\\.x$", "", colnames(full_data))
    
    
    full_data <- full_data %>%
      mutate(
        Rok = ifelse(is.na(Rok), as.integer(format(data_czas, "%Y")), Rok),
        Miesiac = ifelse(is.na(Miesiac), as.integer(format(data_czas, "%m")), Miesiac),
        Dzien = ifelse(is.na(Dzien), as.integer(format(data_czas, "%d")), Dzien),
      )
    
    full_data  <- full_data[, -c(2)] 
    full_data$UI <- as.character(paste0(full_data$`id`, "_", full_data$Rok, "_", full_data$Miesiac, "_", full_data$Dzien))
    
  }
  
  if(rank == "climate"){
    

    
    #zostawilismy tylko dwa ststusy i tutaj te ktore nie maja NA zostaja odfiltrowane
    x <- x[is.na(x$`Status pomiaru TMIN`), ] 
    x <- x[is.na(x$`Status pomiaru TMAX`), ]  
    x <- x[, c(1, 2, 6, 3, 4, 5, 7, 9, 11)]
    #x <- x[, c(2, 1, 8, 5, 6, 7, 9, 11, 13, 3, 4)]
    stacje <- c("LIDZBARK WARMIŃSKI", "OLECKO", "PRZELEWICE", "BIEBRZA-PIEŃCZYKÓWEK", "RÓŻANYSTOK",
                "GORZYŃ", "WIELICHOWO", "KÓRNIK", "KOŁUDA WIELKA", "POŚWIĘTNE", "LEGIONOWO",
                "WARSZAWA-BIELANY", "PUŁTUSK", "SZEPIETOWO", "BIAŁOWIEŻA", "SMOLICE", "PUCZNIEW",
                "SKIERNIEWICE", "PUŁAWY", "KRASNYSTAW", "PSZCZYNA", "SILNICZKA", "KRAKÓW-OBSERWATORIUM",
                "BORUSOWA", "IGOŁOMIA", "STASZÓW", "CHORZELÓW", "STRZYŻÓW", "ZAWOJA", "JABŁONKA",
                "LIMANOWA", "ŁĄCKO", "KROŚCIENKO", "KRYNICA", "MUSZYNA", "DYNÓW", "KOMAŃCZA")
    
    x <- x[x$`Nazwa stacji` %in% stacje, ]
    
    x$data_czas <- make_datetime(year = x$Rok, month = x$Miesiac, day = x$Dzien)   #dodanie kolumny w formacie data
    start_date <- min(x$data)  # początek okresu
    end_date <- max(x$data)    # koniec okresu
    all_dates <- seq(from = start_date, to = end_date, by = "day")
    
    full_data <- expand.grid(
      `id` = unique(x$`id`),data_czas = all_dates) %>%
      left_join(x %>% select(`id`, rank_code = rank_code, `Nazwa stacji` = `Nazwa stacji`) %>%
                  distinct(),
                by = "id")
    # full_data <- expand.grid(
    #   `Kod stacji` = unique(x$`Kod stacji`),data_czas = all_dates) %>%
    #   left_join(x %>% select(`Kod stacji`, rank_code = rank_code, `Nazwa stacji` = `Nazwa stacji`) %>%
    #               distinct(),
    #             by = "Kod stacji")

    full_data <- left_join(full_data, x, by = c("id", "data_czas"))
    full_data <- full_data[, -c(5, 6)] 
    colnames(full_data) <- gsub("\\.x$", "", colnames(full_data))
    
    
    full_data <- full_data %>%
      mutate(
        Rok = ifelse(is.na(Rok), as.integer(format(data_czas, "%Y")), Rok),
        Miesiac = ifelse(is.na(Miesiac), as.integer(format(data_czas, "%m")), Miesiac),
        Dzien = ifelse(is.na(Dzien), as.integer(format(data_czas, "%d")), Dzien),
      )
    
    full_data  <- full_data[, -c(2)] 
    
    full_data$UI <- as.character(paste0(full_data$`id`, "_", full_data$Rok, "_", full_data$Miesiac, "_", full_data$Dzien))
    
  }
  
  return(full_data)

}

###############################################################################################

clean_data_hourly = function(x, rank) {
  
  
  x$`Nazwa stacji` <- trimws(x$`Nazwa stacji`)
  # usuwa statusy
  #cols_to_remove <- grep("^Status", colnames(x))
  #cols_to_keep <- grep("^Status pomiaru TEMP$", colnames(x))
  #cols_to_remove <- setdiff(cols_to_remove, cols_to_keep)
  #x[, cols_to_remove] <- NULL

  
  if(rank == "synop"){
    
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "POZNAŃ" = "POZNAŃ-ŁAWICA"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "WARSZAWA" = "WARSZAWA-OKĘCIE"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "WROCŁAW" = "WROCŁAW-STRACHOWICE"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "KOŁOBRZEG" = "KOŁOBRZEG-DŹWIRZYNO"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "KATOWICE" = "KATOWICE-MUCHOWIEC"))
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "ŁÓDŹ" = "ŁÓDŹ-LUBLINEK"))
    
    x <- x %>% mutate(`Nazwa stacji` = recode(`Nazwa stacji`, "KATOWICE" = "KATOWICE-MUCHOWIEC"))

    synop_stacje <- c("KOŁOBRZEG-DŹWIRZYNO", "KOSZALIN", "USTKA", "ŁEBA", "HEL", "GDAŃSK-RĘBIECHOWO", 
                      "GDAŃSK-ŚWIBNO", "ELBLĄG-MILEJEWO", "KĘTRZYN", "SUWAŁKI", "ŚWINOUJŚCIE", "SZCZECIN", 
                      "RESKO-SMÓLSKO", "SZCZECINEK", "PIŁA", "CHOJNICE", "TORUŃ", "MŁAWA", "OLSZTYN", 
                      "MIKOŁAJKI","OSTROŁĘKA", "BIAŁYSTOK", "GORZÓW WIELKOPOLSKI", "SŁUBICE", "POZNAŃ-ŁAWICA", 
                      "KOŁO", "PŁOCK", "WARSZAWA-OKĘCIE", "SIEDLCE", "TERESPOL", "ZIELONA GÓRA", "LEGNICA", 
                      "LESZNO", "WROCŁAW-STRACHOWICE", "KALISZ", "WIELUŃ", "ŁÓDŹ-LUBLINEK", "SULEJÓW", "KOZIENICE", 
                      "LUBLIN-RADAWIEC", "WŁODAWA", "JELENIA GÓRA", "ŚNIEŻKA", "KŁODZKO", "OPOLE", "RACIBÓRZ", 
                      "CZĘSTOCHOWA", "KATOWICE-MUCHOWIEC", "KRAKÓW-BALICE", "KIELCE-SUKÓW", "TARNÓW", 
                      "RZESZÓW-JASIONKA", "SANDOMIERZ", "ZAMOŚĆ", "BIELSKO-BIAŁA", "ZAKOPANE", "KASPROWY WIERCH", 
                      "NOWY SĄCZ", "KROSNO", "LESKO", "PRZEMYŚL")
    
    # usuwanie wierszy z brakiem pomiaru
    x <- x[is.na(x$`Status pomiaru TEMP`), ] 
    x <- x[, c(1, 2, 3, 4, 5, 6, 7, 21)]
    x <- x[x$`Nazwa stacji` %in% synop_stacje, ]
    x$data_czas <- make_datetime(year = x$Rok, month = x$Miesiac, day = x$Dzien, hour = x$Godzina)   #dodanie kolumny w formacie data
    start_date <- min(x$data)  # początek okresu
    end_date <- max(x$data)    # koniec okresu
    all_dates <- seq(from = start_date, to = end_date, by = "hour")
    
    full_data <- expand.grid(
      `Kod stacji` = unique(x$`Kod stacji`),data_czas = all_dates) %>%
      left_join(x %>% select(`Kod stacji`, rank_code = rank_code, `Nazwa stacji` = `Nazwa stacji`) %>%
                  distinct(),
                by = "Kod stacji")
    
    full_data <- left_join(full_data, x, by = c("Kod stacji", "data_czas"))
    full_data <- full_data[, -c(5, 6)] 
    colnames(full_data) <- gsub("\\.x$", "", colnames(full_data))
    
    
    full_data <- full_data %>%
      mutate(
        Rok = ifelse(is.na(Rok), as.integer(format(data_czas, "%Y")), Rok),
        Miesiac = ifelse(is.na(Miesiac), as.integer(format(data_czas, "%m")), Miesiac),
        Dzien = ifelse(is.na(Dzien), as.integer(format(data_czas, "%d")), Dzien),
        Godzina = ifelse(is.na(Godzina), as.integer(format(data_czas, "%H")), Godzina)
      )
    
    
    full_data <- full_data[full_data$Godzina == 6 | full_data$Godzina == 18, ]
    
    
    full_data$data_czas <- NULL # przez ta kolumne robil sie blad
    full_data <- full_data %>% pivot_wider(names_from = `Godzina`, values_from = `Temperatura powietrza [°C]`, names_prefix = "godzina_")
    
    
    full_data$UI <- as.character(paste0(full_data$`Kod stacji`, "_", full_data$Rok, "_", full_data$Miesiac, "_", full_data$Dzien))
    full_data <- full_data %>% rename(id = `Kod stacji`)
    }
  
  if(rank == "climate"){
    
    
    stacje <- c("LIDZBARK WARMIŃSKI", "OLECKO", "PRZELEWICE", "BIEBRZA-PIEŃCZYKÓWEK", "RÓŻANYSTOK",
                "GORZYŃ", "WIELICHOWO", "KÓRNIK", "KOŁUDA WIELKA", "POŚWIĘTNE", "LEGIONOWO",
                "WARSZAWA-BIELANY", "PUŁTUSK", "SZEPIETOWO", "BIAŁOWIEŻA", "SMOLICE", "PUCZNIEW",
                "SKIERNIEWICE", "PUŁAWY", "KRASNYSTAW", "PSZCZYNA", "SILNICZKA", "KRAKÓW-OBSERWATORIUM",
                "BORUSOWA", "IGOŁOMIA", "STASZÓW", "CHORZELÓW", "STRZYŻÓW", "ZAWOJA", "JABŁONKA",
                "LIMANOWA", "ŁĄCKO", "KROŚCIENKO", "KRYNICA", "MUSZYNA", "DYNÓW", "KOMAŃCZA")
    
    x <- x[is.na(x$`Status pomiaru TEMP`), ] 
    x <- x[, c(1, 2, 3, 4, 5, 6, 7, 8)]
    x <- x[x$`Nazwa stacji` %in% stacje, ]
    
    x$data_czas <- make_datetime(year = x$Rok, month = x$Miesiac, day = x$Dzien, hour = x$Godzina)   #dodanie kolumny w formacie data
    start_date <- min(x$data)  # początek okresu
    end_date <- max(x$data)    # koniec okresu
    all_dates <- seq(from = start_date, to = end_date, by = "hour")
    
    full_data <- expand.grid(
      `Kod stacji` = unique(x$`Kod stacji`),data_czas = all_dates) %>%
      left_join(x %>% select(`Kod stacji`, rank_code = rank_code, `Nazwa stacji` = `Nazwa stacji`) %>%
                  distinct(),
                by = "Kod stacji")
    
    full_data <- left_join(full_data, x, by = c("Kod stacji", "data_czas"))
    full_data <- full_data[, -c(5, 6)] 
    colnames(full_data) <- gsub("\\.x$", "", colnames(full_data))
    
    
    full_data <- full_data %>%
      mutate(
        Rok = ifelse(is.na(Rok), as.integer(format(data_czas, "%Y")), Rok),
        Miesiac = ifelse(is.na(Miesiac), as.integer(format(data_czas, "%m")), Miesiac),
        Dzien = ifelse(is.na(Dzien), as.integer(format(data_czas, "%d")), Dzien),
        Godzina = ifelse(is.na(Godzina), as.integer(format(data_czas, "%H")), Godzina)
      )
    
    
    full_data <- full_data[full_data$Godzina == 6 | full_data$Godzina == 18, ]
    
    
    
    full_data$data_czas <- NULL # przez ta kolumne robil sie blad
    full_data <- full_data %>% pivot_wider(names_from = `Godzina`, values_from = `Temperatura powietrza [°C]`, names_prefix = "godzina_")
    
    
    full_data$UI <- as.character(paste0(full_data$`Kod stacji`, "_", full_data$Rok, "_", full_data$Miesiac, "_", full_data$Dzien))
    full_data <- full_data %>% rename(id = `Kod stacji`)
    }
  
  return(full_data)
}
  
###############################################################################################

join_data_and_add_coors <- function(dobowe, terminowki, rank){

  
  if(rank == "synop"){
    
    dane <- left_join(dobowe, terminowki, by = "UI")
    dane <- dane[, c(1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 9, 10)]
    
    
    #średnia temperatura dobowa 
    dane$`Srednia temperatura dobowa [°C]` <- apply(dane[, c(7, 8, 9, 10)], 1, function(x) {
      if (any(is.na(x))) {
        NA
      } else {
        round(mean(x, na.rm = TRUE), 1)
      }
    })
    colnames(dane) <- gsub(".x", "", colnames(dane))
    
    dane <- dane %>%
      mutate(szer_geo = case_when(
        `Nazwa stacji` %in% c("KOŁOBRZEG-DŹWIRZYNO", "KOSZALIN", "USTKA", "ŁEBA", "HEL", "GDAŃSK-RĘBIECHOWO", 
                              "GDAŃSK-ŚWIBNO", "ELBLĄG-MILEJEWO", "KĘTRZYN", "SUWAŁKI", "ŚWINOUJŚCIE", "SZCZECIN", 
                              "RESKO-SMÓLSKO", "SZCZECINEK", "PIŁA", "CHOJNICE", "TORUŃ", "MŁAWA", "OLSZTYN", 
                              "MIKOŁAJKI","OSTROŁĘKA", "BIAŁYSTOK" ) ~ 54,
        `Nazwa stacji` %in% c( "GORZÓW WIELKOPOLSKI", "SŁUBICE", "POZNAŃ-ŁAWICA", 
                               "KOŁO", "PŁOCK", "WARSZAWA-OKĘCIE", "SIEDLCE", "TERESPOL", "ZIELONA GÓRA", "LEGNICA", 
                               "LESZNO", "WROCŁAW-STRACHOWICE", "KALISZ", "WIELUŃ", "ŁÓDŹ-LUBLINEK", "SULEJÓW", "KOZIENICE", 
                               "LUBLIN-RADAWIEC", "WŁODAWA") ~ 52,
        `Nazwa stacji` %in% c("JELENIA GÓRA", "ŚNIEŻKA", "KŁODZKO", "OPOLE", "RACIBÓRZ", 
                              "CZĘSTOCHOWA", "KATOWICE-MUCHOWIEC", "KRAKÓW-BALICE", "KIELCE-SUKÓW", "TARNÓW", 
                              "RZESZÓW-JASIONKA", "SANDOMIERZ", "ZAMOŚĆ", "BIELSKO-BIAŁA", "ZAKOPANE", "KASPROWY WIERCH", 
                              "NOWY SĄCZ", "KROSNO", "LESKO", "PRZEMYŚL") ~ 50,
        TRUE ~ NA_real_  # jeśli miasto nie pasuje do żadnej grupy, szerokość będzie NA
      ))
    
    dane$szer_mies_dzien <- as.character(paste0(dane$szer_geo, "_", dane$Miesiac, "_", dane$Dzien))
    
    
  }#koniec if
  
  if(rank=="climate"){


    
    dane <- left_join(dobowe, terminowki, by = "UI")
    dane <- dane[, c(1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 9, 10)]

    
    colnames(dane) <- gsub(".x", "", colnames(dane))
    
    dane <- dane %>%
      mutate(szer_geo = case_when(
        `Nazwa stacji` %in% c("LIDZBARK WARMIŃSKI", "OLECKO", "PRZELEWICE", "BIEBRZA-PIEŃCZYKÓWEK", "RÓŻANYSTOK") ~ 54,
        `Nazwa stacji` %in% c("GORZYŃ", "WIELICHOWO", "KÓRNIK", "KOŁUDA WIELKA", "POŚWIĘTNE", "LEGIONOWO", "WARSZAWA-BIELANY",
                              "PUŁTUSK", "SZEPIETOWO", "BIAŁOWIEŻA", "SMOLICE", "PUCZNIEW", "SKIERNIEWICE", "PUŁAWY") ~ 52,
        `Nazwa stacji` %in% c( "KRASNYSTAW", "PSZCZYNA", "SILNICZKA", "KRAKÓW-OBSERWATORIUM","BORUSOWA", "IGOŁOMIA",
                               "STASZÓW", "CHORZELÓW", "STRZYŻÓW", "ZAWOJA", "JABŁONKA","LIMANOWA", "ŁĄCKO", "KROŚCIENKO",
                               "KRYNICA", "MUSZYNA", "DYNÓW", "KOMAŃCZA") ~ 50,
        TRUE ~ NA_real_  # jeśli miasto nie pasuje do żadnej grupy, szerokość będzie NA
      ))
    
    dane$szer_mies_dzien <- as.character(paste0(dane$szer_geo, "_", dane$Miesiac, "_", dane$Dzien))
    
    
  } #koniec if
  return(dane)
}

###############################################################################################

count_NA_per_year <- function(x, rank){
  
  years <- unique(x$Rok)
  for(i in seq(unique(x$Rok))){
    
    data_year <- filter(x, Rok == years[i])
    na_count_per_station <- aggregate(is.na(Eto) ~ `Nazwa stacji`, data = data_year, sum)
    colnames(na_count_per_station) <- c("Nazwa stacji", "NA_Count_ETO")
    
    View(na_count_per_station, title = paste0("NA Count per Station for ", years[i], rank))
  }
}

###############################################################################################

# ta funkcja bedzie dzielic dane na lata i jeśli w miastach bedzie brakowac 15% danych co dla roku
# wychodzi 54 dni to bedzie odfiltrowana dla tego roku, a jesli nie to przejdzie do nastepnego etapu 

odfiltruj_dane_85_procent <- function(x){
  
  all_data = NULL
  
  years <- unique(x$Rok)
  for(i in seq(unique(x$Rok))){
    year_to_filter <- filter(x, Rok == years[i])
    na_count_per_station <- aggregate(is.na(Eto) ~ `Nazwa stacji`, data = year_to_filter, sum)
    colnames(na_count_per_station) <- c("Nazwa stacji", "NA_Count_ETO")
    miasta_z_duza_iloscia_na <- na_count_per_station$`Nazwa stacji`[na_count_per_station$NA_Count_ETO > 54]
    filtered_x <- year_to_filter %>%filter(!(`Nazwa stacji` %in% miasta_z_duza_iloscia_na))
    
    all_data[[length(all_data) + 1]] = filtered_x
  }
  
  all_data <- bind_rows(all_data)
  return(all_data)
  
}



################################################################################################

fill_the_gap <- function(x) {

  
  # Tworzymy kolumnę data (jeśli jeszcze nie istnieje)
  x$data <- make_datetime(year = x$Rok, month = x$Miesiac, day = x$Dzien)
  
  # Zachowujemy dane źródłowe jako 'dane' 
  dane <- x
  
  # Zamieniamy na data.table – znacznie szybsze filtrowanie i operacje
  setDT(dane)
  
  # Lista kolumn do interpolacji
  columns_to_interpolate <- c("Max_temp", "Min_temp", "godzina_6", "godzina_18")
  
  # Lista unikalnych dat (posortowana – dla stabilności)
  unique_dates <- sort(unique(dane$data))
  
  # Iterujemy po kolumnach do interpolacji
  for (col in columns_to_interpolate) {
    
    # Filtrujemy tylko daty, które mają brakujące wartości w tej kolumnie
    dates_with_na <- dane[is.na(get(col)), unique(data)]
    
    for (current_date in dates_with_na) {
      # Filtrowanie danych dla danej daty
      daily_data <- dane[data == current_date]
      
      # Oddziel obserwowane i brakujące
      observed <- daily_data[!is.na(get(col))]
      missing <- daily_data[is.na(get(col))]
      
      # Jeśli są dane do interpolacji
      if (nrow(observed) > 0 && nrow(missing) > 0) {
        
        # Tworzymy SpatialPointsDataFrame
        coordinates(observed) <- ~DLUG_G + SZER_G
        coordinates(missing) <- ~DLUG_G + SZER_G
        proj4string(observed) <- CRS("+proj=longlat +datum=WGS84")
        proj4string(missing) <- CRS("+proj=longlat +datum=WGS84")
        
        # Model IDW (nmax = 4 najbliższe stacje)
        idw_model <- idw(
          formula = as.formula(paste0(col, " ~ 1")),
          locations = observed,
          newdata = missing,
          idp = 2.0,
          nmax = 4
        )
        
        # Wstawienie zinterpolowanych wartości z powrotem
        dane[data == current_date & is.na(get(col)), (col) := idw_model$var1.pred]
      }
    }
  }
  
  # Uzupełniamy średnią temperaturę dobową
  dane[, `Srednia temperatura dobowa [°C]` := ifelse(
    is.na(`Srednia temperatura dobowa [°C]`),
    rowMeans(.SD, na.rm = TRUE),
    `Srednia temperatura dobowa [°C]`
  ), .SDcols = c("Max_temp", "Min_temp", "godzina_6", "godzina_18")]
  
  # Obliczamy ETo
  dane[, Eto := 0.408 * 0.001 *
         (`Srednia temperatura dobowa [°C]` + 17) *
         (Max_temp - Min_temp)^0.724 *
         Ra]
  
  # Zwracamy wynik
  return(dane)
}

################################################################################################

#_____________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________
#_______________________________________FUNKCJE TERMINOWE_____________________________________________________________
#_____________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________
#_____________________________________________________________________________________________________________________

#' Hourly IMGW meteorological data
#'
#' Downloading hourly (meteorological) data from the SYNOP / CLIMATE / PRECIP stations
#' available in the danepubliczne.imgw.pl collection
#'
#' @param rank rank of the stations: "synop" (default), "climate", or "precip"
#' @param year vector of years (e.g., 1966:2000)
#' @param status leave the columns with measurement and observation statuses
#' (default status = FALSE - i.e. the status columns are deleted)
#' @param coords add coordinates of the station (logical value TRUE or FALSE)
#' @param station name or ID of meteorological station(s).
#' It accepts names (characters in CAPITAL LETTERS) or stations' IDs (numeric)
#' @param col_names three types of column names possible: "short" - default,
#' values with shorten names, "full" - full English description,
#' "polish" - original names in the dataset
#' @param allow_failure logical - whether to proceed or stop on failure. By default set to TRUE (i.e. don't stop on error). For debugging purposes change to FALSE
#' @param ... other parameters that may be passed to the 'shortening'
#' function that shortens column names
#' @importFrom XML readHTMLTable
#' @importFrom utils download.file unzip read.csv
#' @importFrom data.table fread
#' @export
#' @return meteorological data for the hourly time interval
#'
#' @examples \donttest{
#'   hourly = meteo_imgw_hourly(rank = "climate", year = 1984)
#'   head(hourly)
#' }
#'


meteo_imgw_hourly = function(rank = "synop",
                             year,
                             status = FALSE,
                             coords = FALSE,
                             station = NULL,
                             col_names = "short",
                             allow_failure = TRUE,
                             ...) {

  if (allow_failure) {
    tryCatch(meteo_imgw_hourly_bp(rank,
                                  year,
                                  status,
                                  coords,
                                  station,
                                  col_names, ...),
             error = function(e){
               message(paste("Potential error(s) found. Problems with downloading data.\n",
                             "\rRun function with argument allow_failure = FALSE",
                             "to see more details"))})
  } else {
    meteo_imgw_hourly_bp(rank,
                         year,
                         status,
                         coords,
                         station,
                         col_names, ...)
  }
}

#' @keywords internal
#' @noRd
#'
#'


meteo_imgw_hourly_bp = function(rank,
                                year,
                                status,
                                coords,
                                station,
                                col_names, ...) {

  
 # rank <- "synop"
  # year <- "2016:2024"
  
  translit = check_locale()
  stopifnot(rank == "synop" | rank == "climate") # dla terminowek tylko synopy i klimaty maja dane
  base_url = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/"
  interval = "hourly"
  interval_pl = "terminowe"
  meta = meteo_metadata_imgw(interval = "hourly", rank = rank)
  rank_pl = switch(rank, synop = "synop", climate = "klimat", precip = "opad")
  temp = tempfile()
  test_url(link = paste0(base_url, "dane_meteorologiczne/", interval_pl, "/", rank_pl, "/"),
           output = temp)
  a = readLines(temp, warn = FALSE)
  unlink(temp)

  ind = grep(readHTMLTable(a)[[1]]$Name, pattern = "/")
  catalogs = as.character(readHTMLTable(a)[[1]]$Name[ind])

  years_in_catalogs = strsplit(gsub(x = catalogs, pattern = "/", replacement = ""), split = "_")
  years_in_catalogs = lapply(years_in_catalogs, function(x) x[1]:x[length(x)])
  ind = lapply(years_in_catalogs, function(x) sum(x %in% year) > 0)             #tu trzeba zmienic na year
  catalogs = catalogs[unlist(ind)] # to sa nasze prawdziwe catalogs do przemielenia

  all_data = NULL

  for (i in seq_along(catalogs)) {
    catalog = gsub(catalogs[i], pattern = "/", replacement = "")

    if (rank == "synop") {
      address = paste0(base_url, "dane_meteorologiczne/terminowe/synop",
                       "/", catalog, "/")

      test_url(link = address, output = temp)
      folder_contents = readLines(temp, warn = FALSE)
      unlink(temp)

      ind = grep(readHTMLTable(folder_contents)[[1]]$Name, pattern = "zip")
      files = as.character(readHTMLTable(folder_contents)[[1]]$Name[ind])

      addresses_to_download = paste0(address, files)

      for (j in seq_along(addresses_to_download)) {

        # if (grepl("2024_10", addresses_to_download[j])) {
        #   next
        # }

        temp = tempfile()
        temp2 = tempfile()
        test_url(addresses_to_download[j], temp)

        unzip(zipfile = temp, exdir = temp2)

        file1 = paste(temp2, dir(temp2), sep = "/")

        if (translit) {
          data1 = as.data.frame(data.table::fread(cmd = paste("iconv -f CP1250 -t ASCII//TRANSLIT", file1)))
        } else {
          data1 = suppressWarnings(read.csv(file1, header = FALSE, stringsAsFactors = FALSE, fileEncoding = "CP1250"))
        }

        colnames(data1) = meta[[1]]$parameters

        # usuwa statusy i pogode bierząca i ubieglą
        cols_to_remove <- grep("^Status", colnames(data1))
        cols_to_keep <- grep("^Status pomiaru TEMP$", colnames(data1))
        cols_to_remove <- setdiff(cols_to_remove, cols_to_keep)
        data1[, cols_to_remove] <- NULL


        #usuwa spaceje przed i po nazwie
        data1$`Nazwa stacji` <- trimws(data1$`Nazwa stacji`)

        unlink(c(temp, temp2))
        all_data[[length(all_data) + 1]] = data1
      } # koniec petli po zipach do pobrania
    } # koniec if'a dla synopa

    ######################
    ###### KLIMAT: #######
    ######################
    if (rank == "climate") {
      address = paste0(base_url, "dane_meteorologiczne/terminowe/klimat",
                       "/", catalog, "/")

      test_url(link = address, output = temp)
      folder_contents = readLines(temp, warn = FALSE)
      unlink(temp)

      ind = grep(readHTMLTable(folder_contents)[[1]]$Name, pattern = "zip")
      files = as.character(readHTMLTable(folder_contents)[[1]]$Name[ind])
      addresses_to_download = paste0(address, files)

      for (j in seq_along(addresses_to_download)) {

        #if (grepl("2024_07", addresses_to_download[j])) {
        #  next
        #}

        temp = tempfile()
        temp2 = tempfile()
        test_url(addresses_to_download[j], temp)
        unzip(zipfile = temp, exdir = temp2)
        file1 = paste(temp2, dir(temp2), sep = "/")

        if (translit) {
          data1 = as.data.frame(data.table::fread(cmd = paste("iconv -f CP1250 -t ASCII//TRANSLIT", file1)))
        } else {
          data1 = read.csv(file1, header = FALSE, stringsAsFactors = FALSE, fileEncoding = "CP1250")
        }
        colnames(data1) = meta[[1]]$parameters


        # usuwa statusy
        cols_to_remove <- grep("^Status", colnames(data1))
        cols_to_keep <- grep("^Status pomiaru TEMP$", colnames(data1))
        cols_to_remove <- setdiff(cols_to_remove, cols_to_keep)
        data1[, cols_to_remove] <- NULL

        data1$`Nazwa stacji` <- trimws(data1$`Nazwa stacji`)

        unlink(c(temp, temp2))
        all_data[[length(all_data) + 1]] = data1
      } # koniec petli po zipach do pobrania
    } # koniec if'a dla klimatu
  } # koniec petli po glownych catalogach danych dobowych

  all_data = do.call(rbind, all_data)


  if (coords) {
    all_data = merge(climate::imgw_meteo_stations[, 1:3],
                     all_data,
                     by.x = "id",
                     by.y = "Kod stacji",
                     all.y = TRUE)
  }

  # dodaje rank
  rank_code = switch(rank, synop = "SYNOPTYCZNA", climate = "KLIMATYCZNA")
  all_data = cbind(data.frame(rank_code = rank_code), all_data)
  all_data = all_data[all_data$Rok %in% year, ] # przyciecie tylko do wybranych lat gdyby sie pobralo za duzo

  #station selection
  if (!is.null(station)) {
    if (is.character(station)) {
      inds = as.numeric(sapply(station, function(x) grep(pattern = x, x = all_data$`Nazwa stacji`)))
      all_data = all_data[inds, ]
      if (nrow(all_data) == 0) {
        stop("Selected station(s) is not available in the database.", call. = FALSE)
      }
    } else if (is.numeric(station)) {
      all_data = all_data[all_data$`Kod stacji` %in% station, ]
      if (nrow(all_data) == 0) {
        stop("Selected station(s) is not available in the database.", call. = FALSE)
      }
    } else {
      stop("Selected station(s) are not in the proper format.", call. = FALSE)
    }
  }

  # sortowanie w zaleznosci od nazw kolumn - raz jest "kod stacji", raz "id"
  if (sum(grepl(x = colnames(all_data), pattern = "Kod stacji"))) {
    all_data = all_data[order(all_data$`Kod stacji`,
                              all_data$Rok,
                              all_data$Miesiac,
                              all_data$Dzien,
                              all_data$Godzina), ]
  } else {
    all_data = all_data[order(all_data$id, all_data$Rok, all_data$Miesiac, all_data$Dzien, all_data$Godzina), ]
  }

  # extra option for shortening colnames and removing duplicates
  all_data = meteo_shortening_imgw(all_data, col_names = col_names, ...)
  rownames(all_data) = NULL
  return(all_data)
}

##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################



#' Check locale
#'
#' This is an extra check for some systems that make use of "C.UTF-8" or any
#' iso-like encoding recognized as causing potential problems;
#' The provided list of checked encoding cannot parse properly characters used
#' in the Polish metservice's repository and therefore will be forced to
#' use ASCII//TRANSLIT
#' @noRd

check_locale = function() {

  if (Sys.getlocale("LC_CTYPE") %in% c("C.UTF-8", "en_US.iso885915")) {
    locale = Sys.getlocale("LC_CTYPE")
    message(paste0("    Your system locale is: ", locale, " which may cause trouble.
    Please consider changing it manually while working with climate, e.g.:
    Sys.setlocale(category = 'LC_ALL', locale = 'en_US.UTF-8') "))
    Sys.sleep(4)
    return(1)
  } else {
    return(0)
  }

}

#' Meteorological metadata
#'
#' Downloading the description (metadata) to the meteorological data available in the dane.imgw repository.imgw.pl.
#' By default, the function returns a list or data frame for a selected subset
#'
#' @param interval temporal resolution of the data ("hourly", "daily", "monthly")
#' @param rank rank of station ("synop", "climate", "precip")
#' @keywords internal
#'
#' @examples
#' \donttest{
#'   #meta = climate:::meteo_metadata_imgw(interval = "hourly", rank = "synop")
#'   #meta = climate:::meteo_metadata_imgw(interval = "daily", rank = "synop")
#'   #meta = climate:::meteo_metadata_imgw(interval = "monthly", rank = "precip")
#' }

meteo_metadata_imgw = function(interval, rank) { # interval can be: monthly, hourly, hourly

  b = NULL
  base_url = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/"

  # METADATA daily:
  if (interval == "daily") { # warning! daily for climates and synop have 2 files with metadata!!!

    if (rank == "synop") {
      b[[1]] = clean_metadata_meteo(address = paste0(base_url,"dane_meteorologiczne/dobowe/synop/s_d_format.txt"),
                                    rank = "synop", interval = "daily")
      b[[2]] = clean_metadata_meteo(address = paste0(base_url, "dane_meteorologiczne/dobowe/synop/s_d_t_format.txt"),
                                    rank = "synop", interval = "daily")
    }

    if (rank == "climate") {
      b[[1]] = clean_metadata_meteo(address = paste0(base_url, "dane_meteorologiczne/dobowe/klimat/k_d_format.txt"),
                                    rank = "climate", interval = "daily")
      b[[2]] = clean_metadata_meteo(address = paste0(base_url, "dane_meteorologiczne/dobowe/klimat/k_d_t_format.txt"),
                                    rank = "climate", interval = "daily")
    }

    if (rank == "precip") {
      b[[1]] = clean_metadata_meteo(address = paste0(base_url, "dane_meteorologiczne/dobowe/opad/o_d_format.txt"),
                                    rank = "precip", interval = "daily")
    }

  } # end of daily interval

  if (interval == "monthly") {

    if (rank == "synop") {
      b[[1]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/miesieczne/synop/s_m_d_format.txt"),
                                    rank = "synop", interval = "monthly")
      b[[2]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/miesieczne/synop/s_m_t_format.txt"),
                                    rank = "synop", interval = "monthly")
    }

    if (rank == "climate") {
      b[[1]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/miesieczne/klimat/k_m_d_format.txt"),
                                    rank = "climate", interval = "monthly")
      b[[2]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/miesieczne/klimat/k_m_t_format.txt"),
                                    rank = "climate", interval = "monthly")
    }

    if (rank == "precip") {
      b[[1]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/miesieczne/opad/o_m_format.txt"),
                                    rank = "precip", interval = "monthly")
    }

  } # koniec MIESIECZNYCH

  ## hourly data section:
  if (interval == "hourly") {
    if (rank == "synop") b[[1]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/terminowe/synop/s_t_format.txt"),
                                                       rank = "synop", interval = "hourly")
    if (rank == "climate") b[[1]] = clean_metadata_meteo(paste0(base_url, "dane_meteorologiczne/terminowe/klimat/k_t_format.txt"),
                                                         rank = "climate", interval = "hourly")
    if (rank == "precip") {
      stop("The precipitation stations ('precip') does not provide hourly data.", call. = FALSE)
    }
  }
  return(b)
}
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################

#' Meteorological metadata cleaning
#'
#' Internal function for meteorological metadata cleaning
#' @param address URL address of the metadata file
#' @param rank stations' rank
#' @param interval temporal interval
#' @importFrom utils read.fwf
#' @importFrom stats na.omit
#' @importFrom stringi stri_trans_general
#' @keywords internal
#'

clean_metadata_meteo = function(address, rank = "synop", interval = "hourly") {

  temp = tempfile()
  test_url(link = address, output = temp)

  # a = readLines(temp, warn = FALSE, encoding = "CP1250") # doesn't work on mac,
  # thus:
  # a = iconv(a, from = "CP1250", to = "ASCII//TRANSLIT")
  a = read.csv(temp, header = FALSE, stringsAsFactors = FALSE,
               fileEncoding = "CP1250")$V1
  a = gsub(a, pattern = "\\?", replacement = "")
  a = stringi::stri_trans_general(a, 'LATIN-ASCII')

  # additional workarounds for mac os but not only...
  a = gsub(x = a, pattern = "'", replacement = "")
  a = gsub(x = a, pattern = "\\^0", replacement = "")
  a = data.frame(V1 = a[nchar(a) > 3], stringsAsFactors = FALSE)
  # this one does not work on windows
  # a = suppressWarnings(na.omit(read.fwf(address, widths = c(1000),
  #                                        fileEncoding = "CP1250", stringsAsFactors = FALSE)))
  length_char = max(nchar(a$V1), na.rm = TRUE)

  if (rank == "precip" && interval == "hourly") length_char = 40 # exception for precip / hourly
  if (rank == "precip" && interval == "daily") length_char = 38 # exception for precip / daily
  if (rank == "synop" && interval == "hourly") length_char = 60 # exception for synop / hourly
  if (rank == "climate" && interval == "monthly") length_char = 52 # exception for climate / monthly

  field = substr(a$V1, length_char - 3, length_char)

  if (rank == "synop" && interval == "monthly") {
    length_char = as.numeric(names(sort(table(nchar(a$V1)), decreasing = TRUE)[1])) + 2
    field = substr(a$V1, length_char - 3, length_char + 2)
  }

  a$field1 = suppressWarnings(as.numeric(unlist(lapply(strsplit(field, "/"), function(x) x[1]))))
  a$field2 = suppressWarnings(as.numeric(unlist(lapply(strsplit(field, "/"), function(x) x[2]))))

  a$V1 = trimws(substr(a$V1, 1, nchar(a$V1) - 3))
  a$V1 = gsub(x = a$V1, pattern = "*  ", "")

  a = a[!(is.na(a$field1) & is.na(a$field2)), ] # remove info about status
  colnames(a)[1] = "parameters"
  return(a)
}

##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################


#' Download file in a graceful way
#'
#' Function for downloading & testing url/internet connection according to CRAN policy
#' Example solution strongly based on https://community.rstudio.com/t/internet-resources-should-fail-gracefully/49199/12
#' as suggested by kvasilopoulos
#'
#' @param link character vector with URL to check
#' @param output character vector for output file name
#' @param quiet logical vector (TRUE or FALSE) to be passed to curl_download function. FALSE by default
#'
#' @importFrom curl curl_download
#' @importFrom curl has_internet
#' @import httr
#'
#' @export
#'
#' @examples
#' \donttest{
#'  link = "https://www1.ncdc.noaa.gov/pub/data/noaa/2019/123300-99999-2019.gz"
#'  output = tempfile()
#'  test_url(link = link, output = output)
#' }
#'



test_url = function(link, output, quiet = FALSE) {
  print(link)
  try_GET = function(x, ...) {
    tryCatch(
      curl::curl_download(url = link, destfile = output, mode = "wb", quiet = quiet, ...),
      error = function(e) conditionMessage(e),
      warning = function(w) conditionMessage(w)
    )
  }
  is_response = function(x) {
    class(x) == "response"
  }

  # First check internet connection
  if (!curl::has_internet()) {
    message("No internet connection! \n")
    return(invisible(NULL))
  }
  # Then try for timeout problems
  resp = try_GET(link)
  if (!is_response(resp)) {
    message(resp)
    return(invisible(NULL))
  }
  # Then stop if status > 400
  if (httr::http_error(resp)) {
    message_for_status(resp)
    message(paste0("\nCheck: ", link, " in your browser!\n"))
    return(invisible(NULL))
  }

}

# b = gracefully_fail("http://httpbin.org/status/404") # http >400
# #> Not Found (HTTP 404).
# gracefully_fail("http://httpbin.org/delay/11") # Timeout
# #> Timeout was reached: [httpbin.org] Operation timed out after 1000 milliseconds with 0 bytes received
# a = gracefully_fail("http://httpbin.org") #OK
#
# b = curl_download(url = "http://httpbin.org", destfile = tempfile())
# b = curl_download(url = "http://httpbin.org/status/404", destfile = tempfile())
#
# url = "http://www2.census.gov/acs2011_5yr/pums/csv_pus.zip"
# test_url(link = url, output = tempfile())



##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################

#' Shortening column names for meteorological variables
#'
#' Shortening column names of meteorological parameters to improve the readability of downloaded dataset from the danepubliczne.imgw.pl collection and removing duplicated column names
#'
#' @param data downloaded dataset with original column names
#' @param col_names three types of column names possible: "short" - default, values with shorten names, "full" - full English description, "polish" - original names in the dataset
#' @param remove_duplicates whether to remove duplicated column names (default TRUE - i.e., columns with duplicated names are deleted)
#' @keywords internal
#'
#' @examples
#' \donttest{
#'   monthly = meteo_imgw("monthly", rank = "climate", year = 1969)
#'   colnames(monthly)
#'   abbr = climate:::meteo_shortening_imgw(data = monthly,
#'       col_names = "full",
#'       remove_duplicates = TRUE)
#'   head(abbr)
#' }
#'

meteo_shortening_imgw = function(data, col_names = "short", remove_duplicates = TRUE) {

  # removing duplicated column names:  (e.g. station's name)
  if (remove_duplicates == TRUE) {
    data = data[, !duplicated(colnames(data))]
    # fix for merged station names with suffixes
    if (any(colnames(data) %in% c("Nazwa stacji.x", "Nazwa stacji.y"))) {
      data$`Nazwa stacji.y` = NULL
      colnames(data)[colnames(data) == "Nazwa stacji.x"] = "Nazwa stacji"
    }

    # fix for mean air temperature which is stated sometimes in two files as:
    # "Srednia dobowa temperatura[°C]" and "Srednia temperatura dobowa [°C]"
    if (any(grepl(x = colnames(data), "Srednia dobowa temperatura"))) {
      data[, which(grepl(x = colnames(data), "Srednia dobowa temperatura"))] = NULL
    }
  }

  if (col_names != "polish") {
    abbrev = climate::imgw_meteo_abbrev
    orig_columns = trimws(gsub("\\s+", " ", colnames(data))) # remove double spaces
    orig_columns = trimws(gsub("\\[.*?]", "", orig_columns)) # remove brackets and content inside

    abbrev$fullname = trimws(gsub("\\[.*?]", "", abbrev$fullname))
    matches = match(orig_columns, abbrev$fullname)
    matches = matches[!is.na(matches)]

    if (col_names == "short") {
      # abbrev english
      colnames(data)[orig_columns %in% abbrev$fullname] = abbrev$abbr_eng[matches]
    }

    if (col_names == "full") {
      # full english names:
      colnames(data)[orig_columns %in% abbrev$fullname] = abbrev$fullname_eng[matches]
    }
  }

  return(data)
}

##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
#' Read IMGW hydrological and meteorological raw files that can be saved in different formats
#'
#' Internal function for reading IMGW files
#' @param translit logical whether translit detected and iconv needed for reading
#' @param fpath path to unzipped CSV-alike file
#'
#' @keywords internal
#' @noRd

imgw_read = function(translit, fpath) {

  if (translit) {
    data = as.data.frame(data.table::fread(cmd = paste("iconv -f ISO-8859-2 -t ASCII//TRANSLIT", fpath)))
  } else {
    data = tryCatch(expr = read.csv(fpath, header = FALSE, stringsAsFactors = FALSE, sep = ",",
                                    fileEncoding = "CP1250"),
                    warning = function(w) {
                      read.csv(fpath, header = FALSE, stringsAsFactors = FALSE, sep = ";")
                    })

    if (ncol(data) == 1) {
      data = tryCatch(expr = read.csv(fpath, header = FALSE, stringsAsFactors = FALSE, sep = ";",
                                      fileEncoding = "UTF-8"),
                      warning = function(w) {
                        read.csv(fpath, header = FALSE, stringsAsFactors = FALSE, sep = ";")
                      })
    }

  }
  return(data)
}

####################################################################################################################

#' meteo_imgw_daily = function(rank = "synop",
#'                             year,
#'                             status = FALSE,
#'                             coords = FALSE,
#'                             station = NULL,
#'                             col_names = "short",
#'                             allow_failure = TRUE,
#'                             ...) {
#' 
#'   if (allow_failure) {
#'     tryCatch(meteo_imgw_daily_bp(rank,
#'                                  year,),
#'              error = function(e){
#'                message(paste("Potential error(s) found. Problems with downloading data.\n",
#'                              "\rRun function with argument allow_failure = FALSE",
#'                              "to see more details"))})
#'   } else {
#'     meteo_imgw_daily_bp(rank,
#'                         year,
#'                         status,
#'                         coords,
#'                         station,
#'                         col_names,
#'                         ...)
#'   }
#' }
#' 
#' #' @keywords internal
#' #' @noRd
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' ##################################################################################################################
#' 
#' 
#' #
#' meteo_imgw_daily_bp = function(rank,
#'                                year,
#'                                ...) {
#' 
#' 
#'   translit = check_locale()
#'   base_url = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/"
#'   interval = "daily"
#'   interval_pl = "dobowe"
#'   meta = meteo_metadata_imgw(interval = "daily", rank = rank)
#'   rank_pl = switch(rank, synop = "synop", climate = "klimat", precip = "opad")
#' 
#'   temp = tempfile()
#'   test_url(link = paste0(base_url, "dane_meteorologiczne/", interval_pl, "/", rank_pl, "/"),
#'            output = temp)
#'   a = readLines(temp, warn = FALSE)
#'   unlink(temp)
#' 
#'   ind = grep(readHTMLTable(a)[[1]]$Name, pattern = "/")
#'   catalogs = as.character(readHTMLTable(a)[[1]]$Name[ind])
#' 
#'   years_in_catalogs = strsplit(gsub(x = catalogs, pattern = "/", replacement = ""),
#'                                split = "_")
#'   years_in_catalogs = lapply(years_in_catalogs, function(x) x[1]:x[length(x)])
#'   ind = lapply(years_in_catalogs, function(x) sum(x %in% year) > 0)
#'   catalogs = catalogs[unlist(ind)]
#' 
#'   all_data = NULL
#' 
#'   for (i in seq_along(catalogs)) {
#'     catalog = gsub(catalogs[i], pattern = "/", replacement = "")
#' 
#'     if (rank == "synop") {
#'       address = paste0(base_url, "/dane_meteorologiczne/dobowe/synop", #nolint
#'                        "/", catalog, "/")
#'       test_url(link = address, output = temp)
#'       folder_contents = readLines(temp, warn = FALSE)
#'       unlink(temp)
#' 
#'       ind = grep(readHTMLTable(folder_contents)[[1]]$Name, pattern = "zip")
#'       files = as.character(readHTMLTable(folder_contents)[[1]]$Name[ind])
#'       addresses_to_download = paste0(address, files)
#' 
#'       for (j in seq_along(addresses_to_download)) {
#' 
#'         temp = tempfile()
#'         temp2 = tempfile()
#'         test_url(addresses_to_download[j], temp)
#'         unzip(zipfile = temp, exdir = temp2)
#' 
#'         file_in_zip = paste(temp2, dir(temp2), sep = "/")
#'         file1 = file_in_zip[1]
#' 
#'         if (translit) {
#'           data1 = as.data.frame(data.table::fread(cmd = paste("iconv -f CP1250 -t ASCII//TRANSLIT", file1)))
#'         } else {
#'           data1 = read.csv(file1, header = FALSE, stringsAsFactors = FALSE, fileEncoding = "CP1250")
#'         }
#'         colnames(data1) = meta[[1]]$parameters
#' 
#' 
#'         if(length(file_in_zip)>1){
#' 
#'           file2 = file_in_zip[2]
#'           if (translit) {
#'             data2 = data.table::fread(cmd = paste("iconv -f CP1250 -t ASCII//TRANSLIT", file2))
#'           } else {
#'             data2 = suppressWarnings(read.csv(file2, header = FALSE, stringsAsFactors = FALSE, fileEncoding = "CP1250"))
#'           }
#'           colnames(data2) = meta[[2]]$parameters
#'         }
#' 
#'         unlink(c(temp, temp2))
#' 
#' 
#'         # usuwa statusy
#'         cols_to_remove <- grep("^Status", colnames(data1))
#'         cols_to_keep <- grep("^Status pomiaru (TMAX|TMIN)$", colnames(data1))
#'         cols_to_remove <- setdiff(cols_to_remove, cols_to_keep)
#'         data1[, cols_to_remove] <- NULL
#' 
#'         if(length(file_in_zip)>1){
#'           data2[grep("^Status", colnames(data2))] = NULL
#' 
#'           ttt = base::merge(data1,
#'                             data2,
#'                             by = c("Kod stacji", "Rok", "Miesiac", "Dzien"),
#'                             all.x = TRUE)
#' 
#'           ttt = ttt[order(ttt$`Nazwa stacji.x`, ttt$Rok, ttt$Miesiac, ttt$Dzien), ]
#'           colnames(ttt)[colnames(ttt) == "Nazwa stacji.x"] = "Nazwa stacji"
#'           ttt = ttt[, !colnames(ttt) %in% "Nazwa stacji.y"]
#'         }
#'         if(length(file_in_zip)==1){
#'           ttt = data1[order(data1$`Nazwa stacji`, data1$Rok, data1$Miesiac, data1$Dzien), ]
#'         }
#' 
#'         ### ta część kodu powtarza sie po dużej petli od rank
#' 
#'         all_data[[length(all_data) + 1]] = ttt
#' 
#'       } # end of looping for zip archives
#'     } # end of if statement for SYNOP stations
#' 
#' 
#'     if (rank == "climate") {
#'       address = paste0(base_url, "dane_meteorologiczne/dobowe/klimat",
#'                        "/", catalog, "/")
#' 
#'       test_url(link = address, output = temp)
#'       folder_contents = readLines(temp, warn = FALSE)
#'       unlink(temp)
#' 
#'       ind = grep(readHTMLTable(folder_contents)[[1]]$Name, pattern = "zip")
#'       files = as.character(readHTMLTable(folder_contents)[[1]]$Name[ind])
#'       addresses_to_download = paste0(address, files)
#' 
#'       for (j in seq_along(addresses_to_download)) {
#' 
#'         if (grepl("2024_07", addresses_to_download[j])) {
#'           next
#'         }
#' 
#'         temp = tempfile()
#'         temp2 = tempfile()
#'         test_url(addresses_to_download[j], temp)
#'         unzip(zipfile = temp, exdir = temp2)
#' 
#'         file_in_zip = paste(temp2, dir(temp2), sep = "/")
#'         file1 = file_in_zip[1]
#' 
#'         if (translit) {
#'           data1 = as.data.frame(data.table::fread(cmd = paste("iconv -f CP1250 -t ASCII//TRANSLIT", file1)))
#'         } else {
#'           data1 = read.csv(file1, header = FALSE, stringsAsFactors = FALSE, fileEncoding = "CP1250")
#'         }
#'         colnames(data1) = meta[[1]]$parameters
#' 
#' 
#'         if(length(file_in_zip)>1){
#' 
#'           file2 = file_in_zip[2]
#'           if (translit) {
#'             data2 = as.data.frame(data.table::fread(cmd = paste("iconv -f CP1250 -t ASCII//TRANSLIT", file2)))
#'           } else {
#'             data2 = read.csv(file2, header = FALSE, stringsAsFactors = FALSE, fileEncoding = "CP1250")
#'           }
#'           colnames(data2) = meta[[2]]$parameters
#'         }
#' 
#'         unlink(c(temp, temp2))
#' 
#'         # usuwa statusy
#'         cols_to_remove <- grep("^Status", colnames(data1))
#'         cols_to_keep <- grep("^Status pomiaru (TMAX|TMIN)$", colnames(data1))
#'         cols_to_remove <- setdiff(cols_to_remove, cols_to_keep)
#'         data1[, cols_to_remove] <- NULL
#' 
#' 
#'         data1$`Nazwa stacji` <- trimws(data1$`Nazwa stacji`)
#' 
#'         if(length(file_in_zip)>1){
#'           data2[grep("^Status", colnames(data2))] = NULL
#'           data2$`Nazwa stacji` <- trimws(data2$`Nazwa stacji`)
#' 
#'           ttt = base::merge(data1,
#'                             data2,
#'                             by = c("Kod stacji", "Rok", "Miesiac", "Dzien"),
#'                             all.x = TRUE)
#' 
#'           ttt = ttt[order(ttt$`Nazwa stacji.x`, ttt$Rok, ttt$Miesiac, ttt$Dzien), ]
#'           colnames(ttt)[colnames(ttt) == "Nazwa stacji.x"] = "Nazwa stacji"
#'           ttt = ttt[, !colnames(ttt) %in% "Nazwa stacji.y"]
#'         }
#' 
#'         if(length(file_in_zip)==1){
#'           ttt = data1[order(data1$`Nazwa stacji`, data1$Rok, data1$Miesiac, data1$Dzien), ]
#'         }
#' 
#'         all_data[[length(all_data) + 1]] = ttt
#' 
#'       } # end of looping for zip files
#'     } # end of if statement for climate stations
#' 
#'     if (rank == "precip") {
#'       address = paste0(base_url, "dane_meteorologiczne/dobowe/opad",
#'                        "/", catalog, "/")
#' 
#'       test_url(link = address, output = temp)
#'       folder_contents = readLines(temp, warn = FALSE)
#'       unlink(temp)
#' 
#'       ind = grep(readHTMLTable(folder_contents)[[1]]$Name, pattern = "zip")
#'       files = as.character(readHTMLTable(folder_contents)[[1]]$Name[ind])
#'       addresses_to_download = paste0(address, files)
#' 
#'       for (j in seq_along(addresses_to_download)) {
#'         temp = tempfile()
#'         temp2 = tempfile()
#'         test_url(addresses_to_download[j], temp)
#'         unzip(zipfile = temp, exdir = temp2)
#'         file1 = paste(temp2, dir(temp2), sep = "/")[1]
#'         data1 = imgw_read(translit, file1)
#'         colnames(data1) = meta[[1]]$parameters
#'         # remove status
#' 
#'         data1[grep("^Status", colnames(data1))] = NULL
#' 
#' 
#'         unlink(c(temp, temp2))
#'         all_data[[length(all_data) + 1]] = data1
#'       } # end of loop for zip files
#'     } # end of if statement for opad stations
#'   } # end of looping over catalogs
#' 
#' 
#'   all_data <- bind_rows(all_data)
#'   all_data$`Nazwa stacji` <- trimws(all_data$`Nazwa stacji`)
#'   if(rank == 'synop' | rank == 'climate'){
#'     rank_code = switch(rank, synop = "SYNOPTYCZNA", climate = "KLIMATYCZNA")
#'     all_data = cbind(data.frame(rank_code = rank_code), all_data)
#'   }
#' 
#'   return(all_data)
#' 
#' }
#' 
#' 
#' 
