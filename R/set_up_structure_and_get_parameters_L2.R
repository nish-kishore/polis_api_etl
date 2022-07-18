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
      "latest_date"=as_date("1900-01-01"),
      "date_field" = "N/A"
    ) %>%
      write_rds(cache_file)
  }
  #create specs yaml
  specs_yaml <- file.path(folder,'cache_dir','specs.yaml')
  if(!file.exists(specs_yaml)){
    if(is.null(token)){token <- ''}
    yaml_out <- list()
    yaml_out$polis$token <- token
    yaml_out$polis_data_folder <- folder
    write_yaml(yaml_out, specs_yaml)
  }
  Sys.setenv("polis_data_folder" = folder)
  if(token != "" & !is.null(token)){
    Sys.setenv("token" = token)
  }
}

#Check to see if table exists in cache_dir. If not, last-updated and last-item-date are default min and entry is created; if table exists no fx necessary
init_polis_data_table <- function(table_name = table_name,
                                  field_name = field_name){
  folder <- load_specs()$polis_data_folder
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  #If there is no cache entry for the requested table then create it: 
  if(nrow(read_cache(.file_name = table_name)) == 0){
    #Create cache entry
    readRDS(cache_file) %>%
      bind_rows(tibble(
        "created"=Sys.time(),
        "updated"=as_date("1900-01-01"), #default date is used in initial POLIS query. It should be set to a value less than the min expected value. Once initial table has been pulled, the date of last pull will be saved here
        "file_type"="rds", #currently, hard-coded as RDS. Could revise to allow user to specify output file type (e.g. rds, csv, other)
        "file_name"= table_name,
        "latest_date"=as_date("1900-01-01"), #default date is used in initial POLIS query. It should be set to a value less than the min expected value. Once initial table has been pulled, the latest date in the table will be saved here
        "date_field" = field_name
      )) %>%
      write_rds(cache_file)
  }
}

#Read cache_dir and return table-name, last-update, and latest-date to the working environment
read_table_in_cache_dir <- function(table_name){
  folder <- load_specs()$polis_data_folder
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  #if a row with the table_name exists within cache, then pull the values from that row
  if(nrow(read_cache(.file_name = table_name)) != 0){
    updated <- (readRDS(cache_file) %>%
                  filter(file_name == table_name))$updated %>%
      as.Date()
    latest_date <- (readRDS(cache_file) %>%
                      filter(file_name == table_name))$latest_date %>%
      as.Date()
    table_name <- (readRDS(cache_file) %>%
                     filter(file_name == table_name))$file_name
    field_name <- (readRDS(cache_file) %>%
                     filter(file_name == table_name))$date_field
  }
  return(list = list("updated" = updated, "latest_date" = latest_date, "field_name" = field_name))
}

#' Read cache and return information
#' @param .file_name A string describing the file name for which you want information
#' @return tibble row which can be atomically accessed
read_cache <- function(.file_name = table_name,
                       cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')){
  readRDS(cache_file) %>%
    filter(file_name == .file_name)
}

#Get user input for which table to pull, if not specified:
get_query_parameters <- function(table_name, 
                              field_name, 
                              id_vars, 
                              download_size,
                              table_name_descriptive,
                              check_for_deleted_rows){
  #Get user input for which table to pull if not specified
  if(is.null(table_name) | is.null(field_name) | is.null(id_vars) | is.null(download_size)){
    defaults <- load_defaults()
    table_list <- c((defaults %>%
                       filter(!grepl("Indicator", table_name) & !grepl("RefData", table_name)))$table_name_descriptive, "Indicator", "Reference Data")
    indicator_list <- (defaults %>%
                         filter(grepl("Indicator", table_name) & !grepl("RefData", table_name)) %>%
                         mutate(table_name_descriptive = str_remove(table_name_descriptive, "Indicator: ")))$table_name_descriptive
    reference_list <- (defaults %>%
                         filter(grepl("RefData", table_name)) %>%
                         mutate(table_name_descriptive = str_remove(table_name_descriptive, "Reference Data: ")))$table_name_descriptive
    table_input <- table_list[utils::menu(table_list, title="Select a POLIS table to download:")]
    if(table_input == "Indicator"){
      indicator_input <- paste0("Indicator: ", indicator_list[utils::menu(indicator_list, title="Select an Indicator to download:")])
      table_defaults <- defaults %>%
        filter(indicator_input == table_name_descriptive)
    }
    if(table_input == "Reference Data"){
      reference_input <- paste0("Reference Data: ", reference_list[utils::menu(reference_list, title="Select a Reference Table to download:")])
      table_defaults <- defaults %>%
        filter(reference_input == table_name_descriptive)
    }
    if(!(table_input %in% c("Indicator", "Reference Data"))){
      table_defaults <- defaults %>%
        filter(table_input == table_name_descriptive)
    }
    table_name <- table_defaults$table_name
    field_name <- table_defaults$field_name
    id_vars <- table_defaults$id_vars
    download_size <- as.numeric(table_defaults$download_size)
  }
  query_parameters <- list(table_name = table_name, 
                           field_name = field_name, 
                           id_vars = id_vars, 
                           download_size = download_size,
                           table_name_descriptive = table_name_descriptive,
                           check_for_deleted_rows = check_for_deleted_rows)
  
  # #Create cache entry and blank dataframe for a POLIS data table if it does not already exist
  # init_polis_data_table(query_parameters$table_name, query_parameters$field_name)
  # 
  #Read the cache entry for the requested POLIS data table
  
  query_parameters <- append(query_parameters, list(updated = read_table_in_cache_dir(query_parameters$table_name)$updated))
  query_parameters <- append(query_parameters, list(latest_date = read_table_in_cache_dir(query_parameters$table_name)$latest_date))
  query_parameters <- append(query_parameters, list(prior_field_name = read_table_in_cache_dir(query_parameters$table_name)$field_name))
  
  #create query_parameters yaml
  query_parameters_yaml <- file.path(load_specs()$polis_data_folder,'cache_dir','query_parameters.yaml')
  yaml_out <- list()
  yaml_out$table_name <- query_parameters$table_name
  yaml_out$field_name <- query_parameters$field_name
  yaml_out$id_vars <- query_parameters$id_vars
  yaml_out$download_size <- query_parameters$download_size
  yaml_out$table_name_descriptive <- query_parameters$table_name_descriptive
  yaml_out$check_for_deleted_rows <- query_parameters$check_for_deleted_rows
  yaml_out$updated <- query_parameters$updated
  yaml_out$latest_date <- query_parameters$latest_date
  yaml_out$prior_field_name <- query_parameters$prior_field_name
  write_yaml(yaml_out, query_parameters_yaml)
  
}



#Run a simple API call to check if the user-provided token is valid
validate_token <- function(token = token){
  my_url_test <-  paste0('https:/extranet.who.int/polis/api/v2/',
                         'Case?',
                         "$inlinecount=allpages&$top=0",
                         '&token=',token) %>%
    httr::modify_url()
  
  response <- call_url(url=my_url_test,
                       error_action = "STOP")
  
  result_test <- as.character(response$status)
  valid_token <- TRUE
  if(result_test != "200"){
    valid_token <- FALSE
  }
  return(valid_token)
}



