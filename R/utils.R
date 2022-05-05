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

#fx2: read cache_dir and return table-name, last-update, and latest-date to the working environment
read_table_in_cache_dir <- function(folder, table_name){
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  if(nrow(read_cache(.file_name = table_name)) != 0){
      updated <<- (readRDS(cache_file) %>%
        filter(file_name == table_name))$updated %>%
        as.Date()
      latest_date <<- (readRDS(cache_file) %>%
        filter(file_name == table_name))$latest_date %>%
        as.Date()
      table_name <<- (readRDS(cache_file) %>%
                         filter(file_name == table_name))$file_name
  }
}

#fx3: take the outputs from fx2 and return the structured API URL string
#date_min_conv (Note: this is copied from idm_polis_api)

#' @param field_name The name of the field used for date filtering.
#' @param date_min The 10 digit string for the min date.
#' @return String compatible with API v2 syntax.
date_min_conv = function(field_name, date_min){
  if(is.null(field_name) || is.null(date_min)) return(NULL)
  paste0("(",
         paste0(field_name, " ge DateTime'",
                date_min, "'"),
         ")")
}

#date_max_conv (Note: this is copied from idm_polis_api)

#' @param field_name The name of the field used for date filtering.
#' @param date_max The 10 digit string for the max date.
#' @return String compatible with API v2 syntax.
date_max_conv = function(field_name, date_max){
  if(is.null(field_name) || is.null(date_max)) return(NULL)
  paste0("(",
         paste0(field_name, " le DateTime'",
                date_max, "'"),
         ")")
}

#make_url_general (Note: this is copied from idm_polis_api)

#' @param field_name The date field to which to apply the date criteria, unique to each data type.
#' @param min_date Ten digit date string YYYY-MM-DD indicating the minimum date, default 2010-01-01
#' @param max_date Ten digit data string YYYY-MM-DD indicating the maximum date, default NULL.
#' @param ... other arguments to be passed to the api
make_url_general = function(field_name,
                            min_date,
                            max_date,
                            ...){
  
  paste0(c(date_min_conv(field_name, min_date),
           date_max_conv(field_name, max_date)),
         collapse = " and ") %>% 
    paste0(...) %>%
    paste0("&$inlinecount=allpages")
}



create_api_url <- function(folder, table_name, updated, latest_date, field_date){
  filter_url_conv = make_url_general(
    field_date,
    min_date,
    max_date
  )
  token <- load_specs()$polis$token
  my_url <<- paste0('https://extranet.who.int/polis/api/v2/',
                  paste0(table_name, "?"),
                  "$filter=",
                  if(filter_url_conv == "") "" else paste0(filter_url_conv),
                  '&token=',token) %>%
    httr::modify_url()
}
