#Utility functions for the POLIS API ETL

#' Check to see if cache exists, if not create it
#' @param folder A string, the location of the polis data folder 
#' @return A string describing the creation process or errors 
init_polis_data_struc <- function(folder, token){
  #check to see if folder exists, if not create it 
  if(!dir.exists(folder)){
    dir.create(folder)
  }
  #check to see if cache directory exists, if not create it
  cache_dir <- file.path(folder, "cache_dir")
  if(!dir.exists(cache_dir)){
    dir.create(cache_dir)
  }
  #check to see if cache admin rds exists, if not create it 
  cache_file <- file.path(cache_dir, "cache.rds")
  if(!file.exists(cache_file)){
    tibble(
      "created"=Sys.time(),
      "updated"=Sys.time(),
      "file_type"="INIT",
      "file_name"="INIT", 
      "latest_date"=as_date("2010-01-01")
    ) %>%
      write_rds(cache_file)
  }
  #create specs yaml 
  specs_yaml <- file.path(folder,'cache_dir','specs.yaml')
  if(!file.exists(specs_yaml)){
    yaml_out <- list()
    yaml_out$polis$token <- token
    yaml_out$polis_data_folder <- folder
    write_yaml(yaml_out, specs_yaml)
  }
  Sys.setenv("polis_data_folder" = folder)
}

#' Load authorizations and local config 
#' @param file A string 
#' @return auth/config object
load_specs <- function(folder = Sys.getenv("polis_data_folder")){
  specs <- read_yaml(file.path(folder,'cache_dir','specs.yaml'))
  return(specs)
}

#' Read cache and return information
#' @param .file_name A string describing the file name for which you want information
#' @return tibble row which can be atomically accessed
read_cache <- function(.file_name, cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')){
  read_rds(cache_file) %>%
    filter(file_name == .file_name)
}

#' Update cache and return row
#' @param .file_name A string describing the file name for which you want to update information
#' @param .val_to_update A string describing the value to be updated 
#' @param .val A value of the same type as the value you're replacing
#' @return row of a tibble
update_cache <- function(.file_name,
                         .val_to_update,
                         .val,
                         cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
                         ){
  tmp <- read_rds(cache_file)
  tmp[which(tmp$file_name == .file_name),.val_to_update] <- .val
  write_rds(tmp, cache_file)
  print("Cache updated!")
  return(tmp[which(tmp$file_name == .file_name),])
}


#fx1: check to see if table exists in cache_dir. If not, last-updated and last-item-date are default min and entry is created; if table exists no fx necessary
init_polis_data_table <- function(folder, table_name){
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  if(nrow(read_cache(.file_name = table_name)) == 0){
    #Create cache entry  
    readRDS(cache_file) %>%
      bind_rows(tibble(
        "created"=Sys.time(),
        "updated"=Sys.time(),
        "file_type"="rds",
        "file_name"= table_name, 
        "latest_date"=as_date("2010-01-01")
      )) %>%
      write_rds(cache_file)
    #Create empty destination rds
    write_rds(data.frame(matrix(ncol = 0, nrow = 0)), file.path(cache_dir, paste0(table_name, ".rds")))
  }
}
