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
      "latest_date"=as_date("1900-01-01")
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
init_polis_data_table <- function(table_name){
  folder <- load_specs()$polis_data_folder
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
        "latest_date"=as_date("1900-01-01")
      )) %>%
      write_rds(cache_file)
    #Create empty destination rds
    write_rds(data.frame(matrix(ncol = 0, nrow = 0)), file.path(cache_dir, paste0(table_name, ".rds")))
  }
}

#fx2: read cache_dir and return table-name, last-update, and latest-date to the working environment
read_table_in_cache_dir <- function(table_name){
  folder <- load_specs()$polis_data_folder
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



create_api_url <- function(table_name, latest_date, field_date){
  table_name <<- table_name
  latest_date <<- latest_date
  field_date <<- field_date
  min_date <- latest_date
  max_date <- NULL
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

#fx4: Query POLIS via the API url created in fx3

polis_data_pull <- function(my_url, verbose=TRUE){
  all_results <- NULL
  initial_query <- my_url
  i <- 1
  
  #Check if field_date selected has missing values that will be excluded from query, and notify user

  while(!is.null(my_url)){
    cycle_start <- Sys.time()
    result <- httr::GET(my_url)
    result_content <- httr::content(result,type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    all_results <- bind_rows(all_results,mutate_all(result_content$value,as.character))
    table_count <- result_content$odata.count
    my_url <- result_content$odata.nextLink
    #Get total queries on initial pass-through
    if(i == 1){
      total_queries <- ceiling(as.numeric(table_count)/nrow(result_content$value))
      } 
    cycle_end <- Sys.time()
    cycle_time <- round(as.numeric(cycle_end - cycle_start), 1)
    if(latest_date == "1900-01-01" & i == 1){
      my_url2 <-  paste0('https://extranet.who.int/polis/api/v2/',
                         paste0(table_name, "?"),
                        "$inlinecount=allpages&$top=0",
                        '&token=',token) 
      result2 <- httr::GET(my_url2)
      result_content2 <- httr::content(result2, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
      table_count2 <- result_content2$odata.count
      if(as.numeric(table_count2) > as.numeric(table_count)){
        warning(print(paste0("The selected field date ('", field_date, "') includes ", as.numeric(table_count2) - as.numeric(table_count), " obs with missing values. These will be excluded from the dataset.")))
      }
    }
    if(verbose) print(paste0('Completed query ', i, " of ", total_queries, "; Query time: ", cycle_time, " seconds"))
    i <- i + 1
  }
  if(!is.null(result_content$odata.count)){
    if(nrow(all_results) != as.numeric(result_content$odata.count)){
      warning(paste0('Expected ',result_content$odata.count, ' results, returned ',nrow(all_results))) 
    }
  }
  attr(all_results,'query') = initial_query
  return(all_results)
}

# fx5: calculates new last-update and latest-date and enters it into the cache
# fx5 <- function()
