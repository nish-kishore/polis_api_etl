#Utility functions for the POLIS API ETL

#' Load authorizations and local config 
#' @param file A string 
#' @return auth/config object
load_specs <- function(){
  auth <- read_yaml(here('spec','auth.yaml'))
  config <- read_yaml(here('spec','config.yaml'))
  return(c(auth,config))
}
 
#' Check to see if cache exists, if not create it
#' @param folder A string, the location of the polis data folder 
#' @return A string describing the creation process or errors 
create_folder_structure <- function(file = load_specs()$polis_data_folder){
  #check to see if folder exists, if not create it 
  if(!dir.exists(file)){
    dir.create(file)
  }
  #check to see if cache directory exists, if not create it
  cache_dir <- file.path(file, "cache_dir")
  if(!dir.exists(cache_dir)){
    dir.create(cache_dir)
  }
  #check to see if cache admin rds exists, if not create it 
  cache_file <- file.path(cache_dir, "cache.rds")
  if(!file.exists(cache_file)){
    tibble(
      "timestamp"=date
    )
  }
}