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

clean_data_from_SH_API = function(x) {
  
  
  x$`Nazwa.stacji` <- trimws(x$`Nazwa.stacji`)

    
    x$date <- as.Date(x$date)
    start_date <- min(x$date)  # początek okresu
    end_date <- max(x$date)    # koniec okresu
    all_dates <- seq(from = start_date, to = end_date, by = "day")
    
    full_data <- expand.grid(
      `id` = unique(x$`id`),date = all_dates) %>%
      left_join(x %>% select(`id`, rank_code = rank_code, `Nazwa.stacji` = `Nazwa.stacji`) %>%
                  distinct(),
                by = "id")
    
    full_data <- left_join(full_data, x, by = c("id", "date"))
    full_data <- full_data[, -c(10, 11)] 
    colnames(full_data) <- gsub("\\.x$", "", colnames(full_data))
    
    
    full_data <- full_data %>%
      mutate(
        Rok = as.integer(format(date, "%Y")),
        Miesiac = as.integer(format(date, "%m")),
        Dzien = as.integer(format(date, "%d"))
      )
    
    full_data$UI <- as.character(paste0(full_data$`id`, "_", full_data$Rok, "_", full_data$Miesiac, "_", full_data$Dzien))
    
  
  return(full_data)
  
}

################################################################################

add_coors_from_SH_API <- function(x){
  
    
    x <- x %>%
      mutate(szer_geo = case_when(
        `Nazwa.stacji` %in% c("KOŁOBRZEG-DŹWIRZYNO", "KOSZALIN", "USTKA", "ŁEBA", "HEL", "GDAŃSK-RĘBIECHOWO", 
                              "GDAŃSK-ŚWIBNO", "ELBLĄG-MILEJEWO", "KĘTRZYN", "SUWAŁKI", "ŚWINOUJŚCIE", "SZCZECIN", 
                              "RESKO-SMÓLSKO", "SZCZECINEK", "PIŁA", "CHOJNICE", "TORUŃ", "MŁAWA", "OLSZTYN", 
                              "MIKOŁAJKI","OSTROŁĘKA", "BIAŁYSTOK", "LIDZBARK WARMIŃSKI", "OLECKO", 
                              "PRZELEWICE", "BIEBRZA-PIEŃCZYKÓWEK", "RÓŻANYSTOK" ) ~ 54,
        
        `Nazwa.stacji` %in% c( "GORZÓW WIELKOPOLSKI", "SŁUBICE", "POZNAŃ-ŁAWICA", 
                               "KOŁO", "PŁOCK", "WARSZAWA-OKĘCIE", "SIEDLCE", "TERESPOL", "ZIELONA GÓRA", "LEGNICA", 
                               "LESZNO", "WROCŁAW-STRACHOWICE", "KALISZ", "WIELUŃ", "ŁÓDŹ-LUBLINEK", "SULEJÓW", "KOZIENICE", 
                               "LUBLIN-RADAWIEC", "WŁODAWA", "GORZYŃ", "WIELICHOWO", "KÓRNIK", "KOŁUDA WIELKA", "POŚWIĘTNE", 
                               "LEGIONOWO", "WARSZAWA-BIELANY","PUŁTUSK", "SZEPIETOWO", "BIAŁOWIEŻA", "SMOLICE", "PUCZNIEW", 
                               "SKIERNIEWICE", "PUŁAWY") ~ 52,
        
        `Nazwa.stacji` %in% c("JELENIA GÓRA", "ŚNIEŻKA", "KŁODZKO", "OPOLE", "RACIBÓRZ", 
                              "CZĘSTOCHOWA", "KATOWICE-MUCHOWIEC", "KRAKÓW-BALICE", "KIELCE-SUKÓW", "TARNÓW", 
                              "RZESZÓW-JASIONKA", "SANDOMIERZ", "ZAMOŚĆ", "BIELSKO-BIAŁA", "ZAKOPANE", "KASPROWY WIERCH", 
                              "NOWY SĄCZ", "KROSNO", "LESKO", "PRZEMYŚL",  "KRASNYSTAW", "PSZCZYNA", "SILNICZKA", 
                              "KRAKÓW-OBSERWATORIUM","BORUSOWA", "IGOŁOMIA", "STASZÓW", "CHORZELÓW", "STRZYŻÓW", "ZAWOJA", 
                              "JABŁONKA","LIMANOWA", "ŁĄCKO", "KROŚCIENKO", "KRYNICA", "MUSZYNA", "DYNÓW", "KOMAŃCZA") ~ 50,
        TRUE ~ NA_real_  # jeśli miasto nie pasuje do żadnej grupy, szerokość będzie NA
      ))
    
    x$szer_mies_dzien <- as.character(paste0(x$szer_geo, "_", x$Miesiac, "_", x$Dzien))

  return(x)
}

################################################################################

count_NA_per_year_SH_API <- function(x) {
  
  
  years <- unique(x$Rok)
  
  for (i in seq_along(years)) {
    data_year <- dplyr::filter(x, Rok == years[i])
    
    na_count <- data_year %>%
      dplyr::group_by(Nazwa.stacji) %>%
      dplyr::summarise(NA_Count_ETO = sum(is.na(Eto))) %>%
      dplyr::arrange(desc(NA_Count_ETO))
    
    View(na_count, title = paste0("NA Count per Station for ", years[i]))
  }
}

################################################################################

# fill_the_gap_SH_API = function(x){
#   
#   dane <- x  #<----------------------------- to musi zostac
#   
#   # Lista kolumn, które chcemy interpolować
#   columns_to_interpolate <- c(
#     "max",
#     "min",
#     "godz_6",
#     "godz_18"
#   )
#   
#   # Tworzymy obiekt przestrzenny na podstawie współrzędnych
#   coordinates(x) <- ~DLUG_G + SZER_G
#   proj4string(x) <- CRS("+proj=longlat +datum=WGS84")
#   
#   # Iteracja przez kolumny do interpolacji
#   for (col in columns_to_interpolate) {
#     
#     #col <- "Max_temp"   #<------------------------debuging
#     
#     # Iteracja przez każdą unikalną datę
#     for (unique_date in unique(x$date)) {
#       
#       #unique_date <- "1971-01-20"   #<-----------------------debugging
#       
#       # Filtrowanie danych dla konkretnej daty
#       daily_data <- dane[dane$date == unique_date, ]
#       
#       # Dane obserwowane (z wartościami)
#       observed <- daily_data[!is.na(daily_data[[col]]), ]
#       
#       # Dane brakujące (z NA)
#       missing <- daily_data[is.na(daily_data[[col]]), ]
#       
#       # Jeśli są dane brakujące i obserwowane
#       if (nrow(observed) > 0 && nrow(missing) > 0) {
#         
#         # # Zresetuj Spatial (konwersja do data.frame)
#         # observed <- as.data.frame(observed)
#         # missing <- as.data.frame(missing)
#         
#         coordinates(observed) <- ~ DLUG_G + SZER_G
#         coordinates(missing) <- ~ DLUG_G + SZER_G
#         
#         # Model IDW z ograniczeniem do 4 najbliższych stacji
#         idw_model <- idw(
#           formula = as.formula(paste0(col, " ~ 1")),
#           locations = observed,
#           newdata = missing,
#           idp = 2.0,
#           nmax = 4
#         )
#         
#         # Aktualizacja brakujących wartości
#         dane[[col]][dane$date == unique_date & is.na(dane[[col]])] <- idw_model$var1.pred
#       }
#     }
#   }
#   
#   # Uzupełnij średnią temperaturę jeśli trzeba
#   dane$mean <- ifelse(
#     is.na(dane$mean),
#     rowMeans(dane[, c("max", "min", "godz_6", "godz_18")], na.rm = TRUE),
#     dane$mean
#   )
#   
#   # Oblicz ETo
#   dane$Eto <- 0.408 * 0.001 * 
#     (dane$mean + 17) * 
#     (dane$max - dane$min)^0.724 * 
#     dane$Ra
#   
#   dane <- dane[, c(2, 1, 4, 10, 11, 12, 6, 5, 8, 9, 7, 16, 17, 13, 14, 18, 19, 3)]
#   dane <- dane %>%
#     rename(
#       godzina_6 = godz_6,
#       godzina_18 = godz_18,
#       "Srednia temperatura dobowa [°C]" = mean,
#       "Nazwa stacji" = Nazwa.stacji,
#       "Max_temp" = max,
#       "Min_temp" = min,
#       "data" = "date"
#     )
#   
#   return(dane)
# }

###############################################################################

fill_the_gap_SH_API <- function(x) {
  
  #x <- dane_2025_uzupelnione_ze_wspol <------
  
  # Tworzymy kolumnę data (jeśli jeszcze nie istnieje)
  x$data <- make_datetime(year = x$Rok, month = x$Miesiac, day = x$Dzien)
  
  # Zachowujemy dane źródłowe jako 'dane' 
  dane <- x
  
  # Zamieniamy na data.table – znacznie szybsze filtrowanie i operacje
  setDT(dane)
  
  # Lista kolumn do interpolacji
  columns_to_interpolate <- c("max", "min", "godz_6", "godz_18")
  
  # Lista unikalnych dat (posortowana – dla stabilności)
  unique_dates <- sort(unique(dane$data))
  
  # Iterujemy po kolumnach do interpolacji
  for (col in columns_to_interpolate) {
    
    #col <- "min"  <----
    
    # Filtrujemy tylko daty, które mają brakujące wartości w tej kolumnie
    dates_with_na <- dane[is.na(get(col)), unique(data)]
    
    for (current_date in dates_with_na) {
      
      #current_date <- "2025-05-17" <-----
      
      # Filtrowanie danych dla danej daty
      daily_data <- dane[dane$data == current_date, ]
      
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
      print(idw_model$var1.pred)
    }
  }
  
  # Uzupełniamy średnią temperaturę dobową
  dane[, `mean` := ifelse(
    is.na(`mean`),
    rowMeans(.SD, na.rm = TRUE),
    `mean`
  ), .SDcols = c("max", "min", "godz_6", "godz_18")]
  
  # Obliczamy ETo
  dane[, Eto := 0.408 * 0.001 *
         (`mean` + 17) *
         (max - min)^0.724 *
         Ra]
  
  # Zwracamy wynik
  return(dane)
}