#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Title: NC Well Data Cleaning for Geocoding (Alamanace County)
# Coder: C. Nathan Jones (natejones@ua.edu)
# Date: 2/11/2025
# Subject: Prep scraped data for GeoCoding in ArcGIS
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# 1.0 Setup workspace ----------------------------------------------------------

#Clear memory
remove(list = ls())

# install.packages("remotes")
#remotes::install_github("slu-openGIS/postmastr")

#Load packages
library(tidyverse)
library(lubridate)
library(tidytext)
library(postmastr)
library(tidycensus)


#download and tidy data
df <- read_csv('temp/alamance_scraping.csv') 

# 2.0 Parse data ---------------------------------------------------------------
# Function to remove the name (if exists) from the string
remove_name <- function(address) {
  #If NA
  if (is.na(address)) {
    return(NA_character_)  # Return NA if the input is NA
  }
  
  #Break address into parts
  parts <- str_split(address, ";")[[1]]
  
  # Check if the first component starts with a number (indicating it's likely an address)
  if (!str_detect(parts[1], "^[0-9]")) {
    # If it doesn't start with a number, assume it's a name and remove it
    parts <- parts[-1]  # Remove the first part (the name)
  }
  
  # Reconstruct the string without the name
  return(paste(parts, collapse = ";"))
}

# Apply the function to remove names
df<-df %>%
  mutate(scraped_info = address) %>% 
  mutate(address = map_chr(address, remove_name))

#Create address list
df <- df %>% pm_identify(var='address') 

#Parse state and zip
output <- df %>% 
  pm_prep(var = 'address', type='street') %>% 
  pm_postal_parse() %>% 
  pm_state_parse() 

#Create City List
city_list <- pm_dictionary(type = "city", filter = "NC", locale = "us")

#Identify records without city
pm_city_none(output, dictionary = city_list) %>% View()

#Add Elon College and Snow camp to dictionary
missing_city <- pm_append(
  type = "city", 
  input = c('Elon College', 'Snow Camp'), 
  output = c('Elon College', 'Snow Camp'), locale= "us")

#update City Dictionary
city_list <- pm_dictionary(
  type="city", 
  filter = "NC", 
  append = missing_city, 
  locale = "us")

#Parse City
output <- output %>% pm_city_parse(dictionary = city_list)

#Remove dates from city name
output <- output %>% mutate(pm.address = str_remove(pm.address, "^.*;\\s*"))

#Remove erronious zip codes
output <- output %>% 
  mutate(pm.zip = if_else(nchar(pm.zip) > 5, NA_character_, pm.zip))

#Left Join and tidy
df <- left_join(df, output) 

#Define pre-geocode error col
df <- df %>%
  mutate(scraping_error = if_any(c(pm.address, pm.city, pm.state, pm.zip), is.na) %>% as.integer())

#Tidy
df<-df %>% 
  mutate(state = "NC") %>% 
  mutate(county = "Alamance") %>% 
  select(
    sample_id, 
    scraped_info,
    address_full = address, 
    street_address = pm.address, 
    city = pm.city, 
    county,
    state, 
    zip = pm.zip,
    scraping_error)

#Export
write_csv(df, "temp//alamance_cleaning.csv")

#some stats
sum(df$scraping_error)
   