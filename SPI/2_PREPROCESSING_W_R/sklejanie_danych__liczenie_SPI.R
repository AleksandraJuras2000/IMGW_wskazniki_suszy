library(dplyr)
library(openxlsx)
library(writexl)
library(lubridate)
library(SPEI)
library(readxl)


############################### STEP 1 ŁACZENIE DANYCH ####################################################

sciezka_stare_dane <- "../0_Dane_do_SPI/opad_mies_1971_202504_stare do usuniecia.csv" #<--- zmiana ścieżek pamietać aby tu zaktualizować nazwe po policzniu spi 
sciezka_nowe_dane <- "../0_Dane_do_SPI/opad_mies_20250618.csv"                    #<----zmiana ścieżek #tu jest plik z sh api z podbanymi nowymi opadami



#zaczytujemy stare dane
dane_stare <- read.csv(sciezka_stare_dane, sep = ",")

# zaczytujemy nowe dane
dane_nowe <- read.csv(sciezka_nowe_dane, sep = ";") 

#dodanie nazwy stacji do nowych danych
kody <- read.csv("kody_SZS_CBDH.csv", row.names = 1)
dane_nowe <- merge(dane_nowe, kody, by.x = "dre.name", by.y = "KOD_SZS")

#dodanie kolumny z data agregacji do nowych danych
dane_nowe  <- dane_nowe  %>% mutate(aggregation_date = as.Date(date) %m-% months(1))

#wyczyszczenie tabeli z niepotrzebnych kolumn
dane_nowe  <- dane_nowe[, c(4,1, 6, 3)]

#usuwanie z dane_stare niepotrzebnej kolumny X
dane_stare$X <- NULL

#zmieniana formatu kolumny aggregation_date z character na date
dane_stare  <- dane_stare  %>% mutate(aggregation_date = as.Date(aggregation_date))

#sklejanie obu tabel stara z nową
tabela_polaczona <- bind_rows(dane_stare, dane_nowe)

# sprawdzamy w agregacji ile stacji ma kompletne ciągi danych w połączonych danych
agrr <- tabela_polaczona %>% group_by(dre.name) %>% summarise(record_count = n())

# <--- Final step  na podstawie agregacji odfiltrowujemy niekompletne dane
tabela_polaczona_final <- tabela_polaczona %>% filter(dre.name %in% agrr$dre.name[agrr$record_count == max(agrr$record_count)])


# zapisujemy do pliku csv lub xlsx   jest to tabela połaczona, trzeba ją zapisać poniewaz 
# to do tej tabeli bedziemy kiedys doklejac dane
write.csv(tabela_polaczona_final, "../0_Dane_do_SPI/opad_mies_1971_202505.csv") #<---------- zmienic nazwe



######################################## STEP 2 LICZENIE SPI  #######################################################



## SPI (Standardized Precipitation Index) liczy się dla różnych skal czasowych:

# SPI-1 – dla 1 miesiąca (krótkoterminowa suchość/wilgotność)
# SPI-3 – dla 3 miesięcy (często używany do analizy sezonowej)
# SPI-6 – dla 6 miesięcy
# SPI-12 – dla 12 miesięcy (długoterminowe zmiany wilgotności)
# SPI-24 – dla 24 miesięcy (bardzo długoterminowe trendy klimatyczne)


#podmianiamy teraz 0 na 0.01 aby skrypt działał poprawnie.. jesli zostawimy 0 to wyrzuci Inf
tabela_polaczona_final <- tabela_polaczona_final %>% mutate(value = ifelse(value == 0, 0.001, value))


ref_start <- c(1981, 1) #### <--------------- tu trzeba zmienic okres ref
ref_end <- c(2020, 12)  ##### <--------------- tu trzeba zmienic okres ref



# Obliczanie SPI 1, SPI 3, SPI 6, SPI 12, SPI 24 dla każdej stacji w jednej tabeli
spi_result <- tabela_polaczona_final %>%
  group_by(dre.name) %>%
  group_modify(~{
    
    # Upewnij się, że dane są posortowane przed tworzeniem ts()
    .x <- .x %>% arrange(aggregation_date)
    
    start_year <- year(min(.x$aggregation_date))
    start_month <- month(min(.x$aggregation_date))
    
    # Tworzenie szeregu czasowego SPI wymaga uporządkowanych danych!
    ts_data <- ts(.x$value, start = c(start_year, start_month), frequency = 12)
    
    # Obliczanie SPI1 (okres 1-miesięczny)
    spi_values_1 <- spi(ts(.x$value, start = c(start_year, start_month), frequency = 12), scale = 1, ref.start = ref_start, ref.end = ref_end)$fitted
    .x$SPI1 <- as.numeric(spi_values_1)  # Dodanie kolumny SPI1 jako numeric
    
    # Obliczanie SPI3 (okres 3-miesięczny)
    spi_values_3 <- spi(ts(.x$value, start = c(start_year, start_month), frequency = 12), scale = 3, ref.start = ref_start, ref.end = ref_end)$fitted
    .x$SPI3 <- as.numeric(spi_values_3)  # Dodanie kolumny SPI3 jako numeric
    
    # Obliczanie SPI6 (okres 6-miesięczny)
    spi_values_6 <- spi(ts(.x$value, start = c(start_year, start_month), frequency = 12), scale = 6, ref.start = ref_start, ref.end = ref_end)$fitted
    .x$SPI6 <- as.numeric(spi_values_6)  # Dodanie kolumny SPI6 jako numeric
    
    # Obliczanie SPI12 (okres 12-miesięczny)
    spi_values_12 <- spi(ts(.x$value, start = c(start_year, start_month), frequency = 12), scale = 12, ref.start = ref_start, ref.end = ref_end)$fitted
    .x$SPI12 <- as.numeric(spi_values_12)  # Dodanie kolumny SPI12 jako numeric
    
    # Obliczanie SPI24 (okres 24-miesięczny)
    spi_values_24 <- spi(ts(.x$value, start = c(start_year, start_month), frequency = 12), scale = 24, ref.start = ref_start, ref.end = ref_end)$fitted
    .x$SPI24 <- as.numeric(spi_values_24)  # Dodanie kolumny SPI24 jako numeric
    
    return(.x)
  }) %>%
  ungroup()

########################################################################################################

#dodajemy kolumne rok i miesiac
spi_result <- spi_result %>%
  mutate(
    rok = year(aggregation_date),
    miesiac = month(aggregation_date)
  )

spi_result <- spi_result %>% mutate(aggregation_date = as.Date(aggregation_date))
####################################################################################################
#zapis do xlsx
write_xlsx(spi_result, "../0_Dane_do_SPI/SPI_1971_202505.xlsx")  # <----------- tu trzeba zmienic nazwe zaktualizować rok_mies
#zapis do csv
write.csv(spi_result, "../0_Dane_do_SPI/SPI_1971_202505.csv")  # <----------- tu trzeba zmienic nazwe zaktualizować rok_mies


