#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Title: NC Well Data Scraping
# Coder: C. Nathan Jones (natejones@ua.edu)
# Date: 2/11/2025
# Subject: Scrape data from NC Well database website
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# 1.0 Setup workspace ----------------------------------------------------------
#Clear memory
remove(list=ls())

#load libraries of interest
library(tidyverse)
library(stringr)
library(rvest)
library(httr)
library(pdftools)
library(parallel)

#Define URL to scrape
url <- "https://celr.dph.ncdhhs.gov/microBiology?client.rasclientId=566000271EH&filterBy=1&recentDay=5&docFrom=&docTo="

# 2.0 Create list of pdf's to scrape -------------------------------------------
# Read HTML content
df <- read_html(url) %>% html_elements("a") %>% html_attr("href") %>% as_tibble()

#Select html dataset
df <- df %>% filter(str_detect(value, "showReportPublic"))

#Identify uid and website
df <- df %>% 
  #Sample ID based on strng
  mutate(sample_id = str_extract(value, "(?<=No=)[^&]+")) %>% 
  #Website
  mutate(report_url = paste0('https://celr.dph.ncdhhs.gov/',value)) %>% 
  #remove value col
  select(-value)

# 3.0 Create function to scrape addresses --------------------------------------
#Create function to pull address from pdf
address_fun<- function(n){
  
  #load libraries of interest
  library(tidyverse)
  library(stringr)
  library(rvest)
  library(httr)
  library(pdftools)
  
  #define sample_id
  sample_id <- df$sample_id[n]
  
  # Define the URL of the PDF
  pdf_url <- df$report_url[n] 
  
  # Download the PDF
  pdf_file <- paste0("temp//",sample_id,".pdf")
  response <- GET(pdf_url, write_disk(pdf_file, overwrite = TRUE))
  pdf_data <- pdf_data(pdf_file)[[1]] %>% as_tibble()
  
  #Define location in pdf for extraction
  x_min <- pdf_data %>% filter(text == "Name") %>% select(x) %>% pull()
  y_min <- pdf_data %>% filter(text == "Name") %>% select(y) %>% pull()
  y_max <- pdf_data %>% filter(text == "EIN:") %>% select(y) %>% pull()
  
  #redefine y_max for older samples without EIN designation
  if(is_empty(y_max)){
    y_max <- pdf_data %>% filter(text == "StarLiMS") %>% select(y) %>% pull()
  }
  
  #filter to location on pdf
  pdf_data <- pdf_data %>% filter(x>=x_min) %>% filter(y>y_min) %>% filter(y<y_max)
  
  #Reconstruct text
  y_tol <- 2
  address<-pdf_data %>% 
    mutate(y_group = round(y/y_tol)*y_tol) %>% 
    group_by(y_group) %>% 
    arrange(y_group, x) %>% 
    summarise(line_text = paste(text, collapse = " ")) %>% 
    pull(line_text) %>% 
    paste(collapse = ";")
  
  #delete pdf file
  file.remove(pdf_file)
  
  #Output
  tibble(sample_id, address)
  }

#4.0 Run address scraping function ---------------------------------------------
#Create an error handling wrapper function
error_fun<-function(n){
  tryCatch(
    expr = address_fun(n), 
    error = function(e)
      tibble(
        sample_id=df$sample_id[n],
        address= NA)
  )
}  

#setup paralell processing environment
n.cores<- detectCores() - 1

#Create clusters
cl<-makeCluster(n.cores)

#Export data to cluter environments
clusterExport(cl, c("address_fun", "df"))

#Now run function
t0<-Sys.time()
output<-parLapply(
  cl=cl,
  seq(1, nrow(df)),#
  error_fun)
tf<-Sys.time()
tf-t0

#Now, bind rows from list output
output<-output %>% bind_rows()

#Stop the clusters
stopCluster(cl)

#export
write_csv(output, "temp//alamance_scraping.csv")
