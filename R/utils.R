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
read_cache <- function(.file_name = table_name,
                       cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')){
  readRDS(cache_file) %>%
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
  tmp <- readRDS(cache_file)
  tmp[which(tmp$file_name == .file_name),.val_to_update] <- .val
  write_rds(tmp, cache_file)
}


#fx1: check to see if table exists in cache_dir. If not, last-updated and last-item-date are default min and entry is created; if table exists no fx necessary
init_polis_data_table <- function(table_name = table_name,
                                  field_name = field_name){
  folder <- load_specs()$polis_data_folder
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
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

#fx2: read cache_dir and return table-name, last-update, and latest-date to the working environment
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

#fx3: take the outputs from fx2 and return the structured API URL string
#date_min_conv (Note: this is copied from idm_polis_api)

#' @param field_name The name of the field used for date filtering.
#' @param date_min The 10 digit string for the min date.
#' @return String compatible with API v2 syntax.

#convert field_name and date_min to the format needed for API query
date_min_conv <- function(field_name, date_min){
  if(is.null(field_name) || is.null(date_min)) return(NULL)
  paste0("(",
         paste0(field_name, " ge DateTime'",
                date_min, "'"),
         ")")
}

#convert field_name to the format needed for API query for null query

#' @param field_name The name of the field used for date filtering.
#' @return String compatible with API v2 syntax.
date_null_conv <- function(field_name){
  if(is.null(field_name)) return(NULL)
  paste0("(",
         paste0(field_name, " eq null",
                ")"))
}

#make_url_general (Note: this is copied from idm_polis_api, with null added)

#' @param field_name The date field to which to apply the date criteria, unique to each data type.
#' @param min_date Ten digit date string YYYY-MM-DD indicating the minimum date, default 2010-01-01
#' @param ... other arguments to be passed to the api
make_url_general <- function(field_name,
                            min_date){

    paste0("(",
           date_min_conv(field_name, min_date),
         ") or (",
         date_null_conv(field_name), ")") %>%
    paste0("&$inlinecount=allpages")
}

#Join the previously cached dataset for a table to the newly pulled dataset
append_and_save <- function(query_output = query_output,
                            id_vars = id_vars, #id_vars is a vector of data element names that, combined, uniquely identifies a row in the table
                            table_name = table_name,
                            full_idvars_output = full_idvars_output){
  
  id_vars <- as.vector(id_vars)
  
  #remove records that are no longer in the POLIS table from query_output
  query_output <- find_and_remove_deleted_obs(full_idvars_output = full_idvars_output,
                                           new_complete_file = query_output,
                                           id_vars = id_vars)
  #If the newly pulled dataset has any data, then read in the old file, remove rows from the old file that are in the new file, then bind the new file and old file
  if(!is.null(query_output) & nrow(query_output) > 0 & file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
  old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
                mutate_all(.,as.character) %>%
      #remove records that are in new file
      anti_join(query_output, by=id_vars) 
  #remove records that are no longer in the POLIS table from old_polis
  old_polis <- find_and_remove_deleted_obs(full_idvars_output = full_idvars_output,
                                           new_complete_file = old_polis,
                                           id_vars = id_vars)
        
  #check that the combined total row number matches POLIS table row number before appending
    #Get full table size for comparison to what was pulled via API, saved as "table_count2"
    my_url2 <-  paste0('https://extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages&$top=0",
                       '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
    result2 <- httr::GET(my_url2)
    result_content2 <- httr::content(result2, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    table_count2 <- as.numeric(result_content2$odata.count)

    #If the overall number of rows in the table is equal to the rows in the old dataset (with new rows removed) + the rows in the new dataset, then combine the two and save
    # if(table_count2 == nrow(old_polis) + nrow(query_output)){
    new_query_output <- query_output %>%
      bind_rows(old_polis)
    #save to file
    write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
    return(new_query_output)
    # }

    #If the overall number of rows in the table is not equal to old and new combined, then stop and flag for investigation
      #NOTE: instead of flagging, this could just trigger a re-pull of the full dataset
      if(table_count2 != nrow(old_polis) + nrow(query_output)){
      warning("Table is incomplete: check id_vars and field_name")
  }
}
  if(!is.null(query_output) & nrow(query_output) > 0 & !file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
    #check that the combined total row number matches POLIS table row number before appending
    #Get full table size for comparison to what was pulled via API, saved as "table_count2"
    my_url2 <-  paste0('https://extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages&$top=0",
                       '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
    result2 <- httr::GET(my_url2)
    result_content2 <- httr::content(result2, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    table_count2 <- as.numeric(result_content2$odata.count)
    
    #If the overall number of rows in the table is equal to the rows in the old dataset (with new rows removed) + the rows in the new dataset, then combine the two and save
    # if(table_count2 == nrow(query_output)){
      new_query_output <- query_output 
      #save to file
      write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
      return(new_query_output)
    # }
    if(table_count2 != nrow(query_output)){
      warning("Table is incomplete: check id_vars and field_name")
  }
}
}

# fx5: calculates new last-update and latest-date and enters it into the cache, saves the dataset as rds
get_update_cache_dates <- function(query_output,
                                   field_name,
                                   table_name){

    #If the newly pulled dataset contains any data, then pull the latest_date as the max of field_name in it
    if(!is.null(query_output) && nrow(query_output) > 0 && field_name %in% colnames(query_output)){
    temp <- query_output %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d")) %>%
      as.data.frame()
    latest_date <- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
    }

    #If the newly pulled dataset is empty (i.e. there is no new data since the last pull), then pull the latest_date as the max of field_name in the old dataset
    if((is.null(query_output) | nrow(query_output) == 0) && field_name %in% colnames(read_lines_raw(file.path(load_specs()$polis_data_folder,
                                                                                 paste0(table_name, ".rds")),n_max=100))){
    old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
      mutate_all(.,as.character)
    temp <- old_polis %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d")) %>%
      as.data.frame()
    latest_date <- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
    }
  #If field_name is blank or not in the dataset, save latest_date as NA
  if(!is.null(query_output) && nrow(query_output) > 0 && !(field_name %in% colnames(query_output))){
    latest_date <- NA
  }
  #Save the current system time as the date/time of 'updated' - the date the database was last checked for updates
  updated <- Sys.time()
  update_cache_dates <- as.data.frame(bind_cols(latest_date = latest_date, updated = updated))
  return(update_cache_dates)
}

#Feature: Validate POLIS Pull (#8): Create POLIS validation metadata and store in cache
get_polis_metadata <- function(query_output,
                               table_name,
                               categorical_max = 30){
  if(nrow(query_output)>0){
  #summarise var names and classes
  var_name_class <- skimr::skim(query_output) %>%
    select(skim_type, skim_variable, character.n_unique) %>%
    rename(var_name = skim_variable,
           var_class = skim_type)

  #categorical sets: for categorical variables with <= n unique values, get a list of unique values
  categorical_vars <- query_output %>%
    select(var_name_class$var_name[var_name_class$character.n_unique <= categorical_max]) %>%
    pivot_longer(cols=everything(), names_to="var_name", values_to = "response") %>%
    distinct() %>%
    pivot_wider(names_from=var_name, values_from=response, values_fn = list) %>%
    pivot_longer(cols=everything(), names_to="var_name", values_to="categorical_response_set")

  #Combine var names/classes/categorical-sets into a 'metadata table'
  table_metadata <- var_name_class %>%
    select(-character.n_unique) %>%
    left_join(categorical_vars, by=c("var_name"))
  }
  if(nrow(query_output) == 0){
    table_metadata <- NULL
  }
  return(table_metadata)
}

#Compare metadata of newly pulled dataset to cached metadata
metadata_comparison <- function(new_table_metadata,
                             old_table_metadata){
  #if new or old metadata are null, go to end
  if(!is.null(new_table_metadata) & !is.null(old_table_metadata)){
    if(nrow(new_table_metadata) != 0 & nrow(old_table_metadata) != 0){
    #compare to old metadata
    compare_metadata <- old_table_metadata %>%
      full_join(new_table_metadata, by=c("var_name"))

    new_vars <- (compare_metadata %>%
      filter(is.na(var_class.x)))$var_name

    if(length(new_vars) != 0){
      new_vars <- new_vars
      warning(print("There are new variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'new_vars'"))
    }

    lost_vars <- (compare_metadata %>%
      filter(is.na(var_class.y)))$var_name

    if(length(lost_vars) != 0){
      lost_vars <- lost_vars
      warning(print("There are missing variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'lost_vars'"))
    }

    class_changed_vars <- compare_metadata %>%
      filter(!(var_name %in% lost_vars) &
             !(var_name %in% new_vars) &
              (var_class.x != var_class.y & 
              !is.null(var_class.x) & !is.null(var_class.y))) %>%
      select(-c(categorical_response_set.x, categorical_response_set.y)) %>%
      rename(old_var_class = var_class.x,
             new_var_class = var_class.y)

    if(nrow(class_changed_vars) != 0){
      class_changed_vars <- class_changed_vars
      warning(print("There are variables in the POLIS table with different classes\ncompared to when it was last retrieved\nReview in 'class_changed_vars'"))
    }

    #Check for new responses in categorical variables (excluding new variables and class changed variables that have been previously shown)
    new_response <- compare_metadata %>%
      filter(!(var_name %in% lost_vars) &
             !(var_name %in% new_vars) &
             !(var_name %in% class_changed_vars$var_name) &
             as.character(categorical_response_set.x) != "NULL" &
             as.character(categorical_response_set.y) != "NULL") %>%
      rowwise() %>%
      mutate(same = toString(intersect(categorical_response_set.x, categorical_response_set.y)),
             in_old_not_new = toString(setdiff(categorical_response_set.x, categorical_response_set.y)),
             in_new_not_old = toString(setdiff(categorical_response_set.y, categorical_response_set.x))) %>%
      filter(in_new_not_old != "") %>%
      rename(old_categorical_response_set = categorical_response_set.x,
             new_categorical_response_set = categorical_response_set.y) %>%
      select(var_name, old_categorical_response_set, new_categorical_response_set, same, in_old_not_new, in_new_not_old)

    if(nrow(new_response) != 0){
      new_response <- new_response
      warning(print("There are categorical responses in the new table\nthat were not seen when it was last retrieved\nReview in 'new_response'"))
    }

    #Create an indicator that is TRUE if there has been a change in table structure or content that requires re-pulling of the table
    re_pull_polis_indicator <- FALSE
    if(nrow(new_response) != 0 |
       nrow(class_changed_vars) != 0 |
       length(lost_vars) != 0 |
       length(new_vars) != 0){
      re_pull_polis_indicator <- TRUE
      }
    }
  }
  if(is.null(new_table_metadata) | is.null(old_table_metadata)){
    re_pull_polis_indicator <- FALSE
  }
  change_summary <- list(re_pull_polis_indicator = re_pull_polis_indicator, new_response = new_response, class_changed_vars = class_changed_vars, lost_vars = lost_vars, new_vars = new_vars)
  return(change_summary)
}

#If re_pull_polis_indicator is TRUE, then re-pull the complete table

polis_re_pull <- function(table_name,
                          field_name,
                          re_pull_polis_indicator){

  if(re_pull_polis_indicator == TRUE){
  #delete cache entry
    folder <- load_specs()$polis_data_folder
    cache_dir <- file.path(folder, "cache_dir")
    cache_file <- file.path(cache_dir, "cache.rds")
    #If cache entry exists, then:
      #remove the cache entry and save over the cache file
      #create a new cache entry for the table
      #delete the old saved dataset and replace it with a blank dataset
      if(nrow(read_cache(.file_name = table_name)) != 0){
        readRDS(cache_file) %>%
          filter(file_name != table_name) %>%
        write_rds(cache_file)

      #create new cache entry
      init_polis_data_table(table_name, field_name)

      #delete previous rds
      if(file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
         file.remove(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
      }
  }
  }
}


#Input function using fx1:fx5

#' @param folder      A string, the location of the polis data folder
#' @param token       A string, the token for the API
#' @param table_name  A string, matching the POLIS name of the requested data table
#' @param field_name  A string, the name of the variable in the requested data table used to filter API query
#' @param id_vars     A vector of variables that, in combination, uniquely identify rows in the requested table
#'
get_polis_table <- function(folder = load_specs()$polis_data_folder,
                            token = load_specs()$polis$token,
                            table_name = NULL,
                            field_name = NULL,
                            id_vars = NULL,
                            download_size = NULL,
                            table_name_descriptive = NULL){
  
  #Get user input for which table to pull if not specified
  if(is.null(table_name) | is.null(field_name) | is.null(id_vars) | is.null(download_size)){
    table_defaults <- prompt_user_input()
    table_name <- table_defaults$table_name
    field_name <- table_defaults$field_name
    id_vars <- table_defaults$id_vars
    download_size <- as.numeric(table_defaults$download_size)
  }
  
  #Create POLIS data folder structure if it does not already exist
  init_polis_data_struc(folder, token)
  
  #If cache entry exists for a POLIS data table, check if the field_name is the same as requested
    #If different, then set re-pull indicator to TRUE
        field_name_change <- NULL
        cache_dir <- file.path(folder, "cache_dir")
        cache_file <- file.path(cache_dir, "cache.rds")
        #if a row with the table_name exists within cache, then pull the values from that row
        if(nrow(read_cache(.file_name = table_name)) != 0){
          old_field_name <- (readRDS(cache_file) %>%
                           filter(file_name == table_name))$date_field
          field_name_change <- field_name != old_field_name
        }
  
  #Archive all data files in the POLIS data folder
    archive_last_data(table_name)
  
  #Create cache entry and blank dataframe for a POLIS data table if it does not already exist
    init_polis_data_table(table_name, field_name)

  #Read the cache entry for the requested POLIS data table
  x <- NULL
  x <- read_table_in_cache_dir(table_name)
  
  
  #Create an API URL and use it to query POLIS
  if(check_if_id_exists(table_name, id_vars = "Id") == FALSE |
     x$field_name == "None"){
    urls <- create_url_array(table_name = table_name,
                           field_name = x$field_name,
                           min_date = x$latest_date,
                           download_size = download_size)
  }
  if(check_if_id_exists(table_name, id_vars = "Id") == TRUE &
     x$field_name != "None"){
    print("Pulling ID variables:")
    urls <- create_url_array_id_method(table_name = table_name,
                                     field_name = x$field_name,
                                     min_date = x$latest_date,
                                     id_vars = id_vars)
  }
  print("Pulling all variables:")
  query_start_time <- Sys.time()
  query_output <- pb_mc_api_pull(urls)  
  query_stop_time <- Sys.time()
  query_time <- round(difftime(query_stop_time, query_start_time, units="auto"),0)
  
  if(check_if_id_exists(table_name, id_vars = "Id") == TRUE &
     x$field_name != "None"){
    #get list of IDs within date filter range
    id_list <- readRDS(paste0(load_specs()$polis_data_folder,"/id_list_temporary_file.rds"))
    #filter query_output to the list of IDs
    query_output <- query_output %>%
      filter(Id %in% id_list$Id)
  }
  #If the query produced any output, summarise it's metadata
  new_table_metadata <- NULL
  if(!is.null(query_output) & nrow(query_output) != 0){
    if(is.null(table_name_descriptive)){
      table_name_descriptive <- table_name
    }
    print(paste0("Downloaded ", nrow(query_output)," rows from ",table_name_descriptive," Table in ", query_time[[1]], " ", units(query_time),"."))
    new_table_metadata <- get_polis_metadata(query_output = query_output,
                                           table_name = table_name)
  }

  #If a version of the data table has been previously pulled and saved, read it in and summarize it's metadata for comparison
  old_table_metadata <- NULL
  if(file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds"))) == TRUE){
    old_table_metadata <- get_polis_metadata(query_output = readRDS( file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds"))),
                                             table_name = table_name)
  }
  
  re_pull_polis_indicator <- FALSE

  #If both old and new metadata summaries exist, then compare them. If there are differences, then set the re_pull_polis_indicator to trigger re-pull of entire table
  if(!is.null(old_table_metadata) &
     !is.null(new_table_metadata)){
    re_pull_polis_indicator <- metadata_comparison(new_table_metadata = new_table_metadata,
                        old_table_metadata = old_table_metadata)$re_pull_polis_indicator
  }

  #If field_name has changed then indicate re-pull
  
  if(!is.null(field_name_change)){
    if(field_name_change == TRUE){
      re_pull_polis_indicator <- TRUE
    }
  }

  #If a re-pull was indicated in the metadata comparison, then re-pull the full table
  polis_re_pull(table_name = table_name,
                field_name = field_name,
                re_pull_polis_indicator = re_pull_polis_indicator)
  
  if(re_pull_polis_indicator == TRUE){
    
    #Read the cache entry for the requested POLIS data table
    x <- NULL
    x <- read_table_in_cache_dir(table_name)
    
    #Create an API URL and use it to query POLIS
    if(check_if_id_exists(table_name, id_vars = "Id") == FALSE |
       x$field_name == "None"){
      urls <- create_url_array(table_name = table_name,
                               field_name = x$field_name,
                               min_date = x$latest_date,
                               download_size = download_size)
    }
    if(check_if_id_exists(table_name, id_vars = "Id") == TRUE &
       x$field_name != "None"){
      urls <- create_url_array_id_method(table_name = table_name,
                                         field_name = x$field_name,
                                         min_date = x$latest_date,
                                         id_vars = id_vars)
    }

    query_start_time <- Sys.time()
    query_output <- pb_mc_api_pull(urls)
    query_stop_time <- Sys.time()
    query_time <- round(difftime(query_stop_time, query_start_time, units="auto"),0)

    if(check_if_id_exists(table_name, id_vars = "Id") == TRUE &
       x$field_name != "None"){
      #get list of IDs within date filter range
      id_list <- readRDS(paste0(load_specs()$polis_data_folder,"/id_list_temporary_file.rds"))
      #filter query_output to the list of IDs
      query_output <- query_output %>%
        filter(Id %in% id_list$Id)
    }
    
    if(!is.null(query_output) & nrow(query_output)>0){
      print(paste0("Metadata or field_name changed from cached version: Re-downloaded ", nrow(query_output)," rows from ",table_name_descriptive," Table in ", query_time[[1]], " ", units(query_time),"."))
    }
    }
  #If the temporary id_list file exists then delete it
  if(file.exists(paste0(load_specs()$polis_data_folder, "/id_list_temporary_file.rds"))){
    file.remove(paste0(load_specs()$polis_data_folder, "/id_list_temporary_file.rds"))
  }
  
  #Combine the query output with the old dataset and save
    #Get a list of all obs id_vars in the full table (for removing deletions in append_and_save)
  full_idvars_output <- get_idvars_only(table_name = table_name,
                                        id_vars = id_vars)
    
  new_query_output <- append_and_save(query_output = query_output,
                                      table_name = table_name,
                                      id_vars = id_vars,
                                      full_idvars_output = full_idvars_output)

  #Get the cache dates for the newly saved table
  update_cache_dates <- get_update_cache_dates(query_output = new_query_output,
                         field_name = field_name,
                         table_name = table_name)

  #Update the cache date fields
  update_cache(.file_name = table_name,
               .val_to_update = "latest_date",
               .val = as.Date(update_cache_dates$latest_date), 
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  update_cache(.file_name = table_name,
               .val_to_update = "updated",
               .val = as.POSIXct(update_cache_dates$updated, tz=Sys.timezone(), origin=lubridate::origin),
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  update_cache(.file_name = table_name,
               .val_to_update = "date_field",
               .val = field_name,
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )

  #add cache data as attributes to rds
  add_cache_attributes(table_name) 
  
  #Get change summary comparing final file to latest archived file
  change_summary <- compare_final_to_archive(table_name,
                           id_vars,
                           categorical_max = 30)
  #Save change_summary to cache
  save_change_summary(table_name = table_name, 
                      change_summary = change_summary,
                      change_log_folder = NULL,
                      n_change_log = 30)
}

#' create a URL to collect the count where field_name is not missing
#' @param
get_table_count <- function(table_name,
                           min_date = as_date("1900-01-01"),
                           field_name){

  filter_url_conv <- make_url_general(
    field_name,
    min_date
  )

  if(field_name == "None"){
    my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$inlinecount=allpages",
                     '&token=',load_specs()$polis$token,
                     "&$top=0") %>%
      httr::modify_url()
  }
  if(field_name != "None"){
  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=",
                   if(filter_url_conv == "") "" else paste0(filter_url_conv),
                   '&token=',load_specs()$polis$token,
                   "&$top=0") %>%
    httr::modify_url()
  }
  
  response <- httr::GET(my_url)

  response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer() %>%
    return()
}

#' create an array of URL's for a given table
#' @param table_name string of a table name available in POLIS
#' @param min_date 'date' object of earliest date acceptable for filter
#' @param field_name string of field name used to filter date
#' @param download_size integer specifying # of rows to download
create_url_array <- function(table_name,
                            min_date = x$latest_date,
                            field_name = x$field_name,
                            download_size = as.numeric(download_size)
                            ){
  #first, create a set of urls with the date filter
  filter_url_conv <- make_url_general(
    field_name,
    min_date
  )
  if(field_name == "None"){
    my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$inlinecount=allpages",
                     '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
  }
  if(field_name != "None"){
  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=",
                   if(filter_url_conv == "") "" else paste0(filter_url_conv),
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  }

  table_size <- get_table_count(table_name = table_name,
                                min_date = min_date,
                                field_name = field_name)

  urls <- paste0(my_url, "&$top=", as.numeric(download_size), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(download_size)))
  
  return(urls)

}


#' get table data for a single url request
#' @param url string of a single url
#' @param p used as iterator in multicore processing
get_table_data <- function(url, p){
  status <- 0
  i <- 1
  while(status != 200 | i <= 3){
  p()
  
  response <- httr::GET(url)
  response_data <- NULL
  status <- response$status_code
    if(status == 200){
      response_data <- response %>%
        httr::content(type='text',encoding = 'UTF-8') %>%
        jsonlite::fromJSON() %>%
        {.$value} %>%
        as_tibble() %>%
        mutate_all(., as.character)
    }
  i <- i+1
  }
  return(response_data)
}

#' multicore pull from API
#' @param urls array of URL strings
mc_api_pull <- function(urls){
  p <- progressor(steps = length(urls))

  future_map(urls,get_table_data, p = p) %>%
    bind_rows()
}

#' wrapper around multicore pull to produce progress bars
#' @param urls array of URL strings
pb_mc_api_pull <- function(urls){
  n_cores <- availableCores() - 1
  plan(multicore, workers = n_cores, gc = T)

  with_progress({
    result <- mc_api_pull(urls)
  })
  return(result)
  stopCluster(n_cores)
}

load_defaults <- function(){
  defaults <- as.data.frame(bind_rows(
    c(table_name_descriptive = "Activity", table_name = "Activity", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Case", table_name = "Case", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Environmental Sample", table_name = "EnvSample", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Geography", table_name = "Geography", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP 0 dose percentage for 6M-59M", table_name = "Indicator('AFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP 1 to 2 dose percentage for 6M-59M", table_name = "Indicator('AFP_DOSE_1_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP 3+ doses percentage for 6M-59M", table_name = "Indicator('AFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP cases", table_name = "Indicator('AFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Circulating VDPV case count (all Serotypes)", table_name = "Indicator('cVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Circulating VDPV cace count (all serotypes) - Reporting", table_name = "Indicator('cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Combined Surveillance Indicators category", table_name = "Indicator('SURVINDCAT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Environmental sample circulating VDPV", table_name = "Indicator('ENV_CVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Environmental samples count", table_name = "Indicator('ENV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Environmental Wild samples", table_name = "Indicator('ENV_WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Non polio AFP cases (under 15Y)", table_name = "Indicator('NPAFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending excluded)", table_name = "Indicator('NPAFP_RATE_NOPENDING')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending included)", table_name = "Indicator('NPAFP_RATE')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: NPAFP 0-2 dose percentage for 6M-59M", table_name = "Indicator('NPAFP_DOSE_0_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: NPAFP 0 dose percentage for 6M-59M", table_name = "Indicator('NPAFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: NPAFP 3+ dose percentage for 6M-59M", table_name = "Indicator('NPAFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of 60-day follow-up cases with inadequate stool", table_name = "Indicator('FUP_INSA_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of cases not classified", table_name = "Indicator('UNCLASS_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of cases w/ adeq stools specimens (condition+timeliness)", table_name = "Indicator('NPAFP_SA_WithStoolCond')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of cases with two specimens within 14 days of onset", table_name = "Indicator('NPAFP_SA')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Reported circulating VDPV environmental samples count (all serotypes)", table_name = "Indicator('ENV_cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Reported Wild environmental samples count", table_name = "Indicator('ENV_WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA bOPV campaigns", table_name = "Indicator('SIA_BOPV')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA mOPV campaigns", table_name = "Indicator('SIA_MOPV')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA planned since last case", table_name = "Indicator('SIA_LASTCASE_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA tOPV campaigns", table_name = "Indicator('SIA_TOPV')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Total SIA campaigns", table_name = "Indicator('SIA_OPVTOT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Wild poliovirus case count", table_name = "Indicator('WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Indicator: Wild poliovirus case count - Reporting", table_name = "Indicator('WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowId", download_size = 1000),
    c(table_name_descriptive = "Lab Specimen", table_name = "LabSpecimen", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "LQAS", table_name = "Lqas", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Population", table_name = "Population", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityCategories", table_name = "RefData('ActivityCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityDeletionReason", table_name = "RefData('ActivityDeletionReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityPhases", table_name = "RefData('ActivityPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityPriorities", table_name = "RefData('ActivityPriorities')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityStatuses", table_name = "RefData('ActivityStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityTypes", table_name = "RefData('ActivityTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AdditionalInterventions", table_name = "RefData('AdditionalInterventions')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AgeGroups", table_name = "RefData('AgeGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AllocationStatuses", table_name = "RefData('AllocationStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AwardTypes", table_name = "RefData('AwardTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CalendarTypes", table_name = "RefData('CalendarTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CaseClassification", table_name = "RefData('CaseClassification')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Categories", table_name = "RefData('Categories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CategoryValues", table_name = "RefData('CategoryValues')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CellCultureResultsLRarm", table_name = "RefData('CellCultureResultsLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CellCultureResultsRLRarm", table_name = "RefData('CellCultureResultsRLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Channels", table_name = "RefData('Channels')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContactIsolatedVdpv", table_name = "RefData('ContactIsolatedVdpv')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContactIsolatedWild", table_name = "RefData('ContactIsolatedWild')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContributionFlexibility", table_name = "RefData('ContributionFlexibility')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContributionStatuses", table_name = "RefData('ContributionStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CostCenters", table_name = "RefData('CostCenters')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Currencies", table_name = "RefData('Currencies')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Datasets", table_name = "RefData('Datasets')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DateTag", table_name = "RefData('DateTag')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DelayReason", table_name = "RefData('DelayReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DiagnosisFinal", table_name = "RefData('DiagnosisFinal')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DocumentCategories", table_name = "RefData('DocumentCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: EmergenceGroups", table_name = "RefData('EmergenceGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FinalCellCultureResultAfp", table_name = "RefData('FinalCellCultureResultAfp')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FinalCellCultureResultLab", table_name = "RefData('FinalCellCultureResultLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FinalITDResults", table_name = "RefData('FinalITDResults')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FollowupFindings", table_name = "RefData('FollowupFindings')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FundingPhases", table_name = "RefData('FundingPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Genders", table_name = "RefData('Genders')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Genotypes", table_name = "RefData('Genotypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Geolevels", table_name = "RefData('Geolevels')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ImportationEvents", table_name = "RefData('ImportationEvents')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed because API documentation states there is no data for this table
    c(table_name_descriptive = "Reference Data: IndependentMonitoringReasons", table_name = "RefData('IndependentMonitoringReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: IndependentMonitoringSources", table_name = "RefData('IndependentMonitoringSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: IndicatorCategories", table_name = "RefData('IndicatorCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: IntratypeLab", table_name = "RefData('IntratypeLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Intratypes", table_name = "RefData('Intratypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: LQASClassifications", table_name = "RefData('LQASClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: LQASDenominators", table_name = "RefData('LQASDenominators')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: LQASThresholds", table_name = "RefData('LQASThresholds')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Methodologies", table_name = "RefData('Methodologies')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Objectives", table_name = "RefData('Objectives')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Outbreaks", table_name = "RefData('Outbreaks')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ParalysisSite", table_name = "RefData('ParalysisSite')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: PartialInclusionReason", table_name = "RefData('PartialInclusionReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: PopulationSources", table_name = "RefData('PopulationSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: PosNeg", table_name = "RefData('PosNeg')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PreviousNumberOfDoses", table_name = "RefData('PreviousNumberOfDoses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ProgrammeAreas", table_name = "RefData('ProgrammeAreas')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ReasonMissed", table_name = "RefData('ReasonMissed')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ReasonVaccineRefused", table_name = "RefData('ReasonVaccineRefused')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Reports", table_name = "RefData('Reports')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ResultsElisa", table_name = "RefData('ResultsElisa')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ResultsPCR", table_name = "RefData('ResultsPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ResultsRRTPCR", table_name = "RefData('ResultsRRTPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: RiskLevel", table_name = "RefData('RiskLevel')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SampleCondition", table_name = "RefData('SampleCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Sectors", table_name = "RefData('Sectors')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SecurityStatus", table_name = "RefData('SecurityStatus')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SourceOfIsolate", table_name = "RefData('SourceOfIsolate')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SpecimenSampleType", table_name = "RefData('SpecimenSampleType')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SpecimenSource", table_name = "RefData('SpecimenSource')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: StoolCondition", table_name = "RefData('StoolCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SurveillanceType", table_name = "RefData('SurveillanceType')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: UpdateType", table_name = "RefData('UpdateType')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Vdpv2Clusters", table_name = "RefData('Vdpv2Clusters')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for Vdpv2Clusters reference data'
    c(table_name_descriptive = "Reference Data: VdpvClassifications", table_name = "RefData('VdpvClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: VdpvSources", table_name = "RefData('VdpvSources')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for VdpvSources reference data'
    c(table_name_descriptive = "Reference Data: VirusTypes", table_name = "RefData('VirusTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: VirusWildType", table_name = "RefData('VirusWildType')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: YesNo", table_name = "RefData('YesNo')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Sub Activity", table_name = "SubActivity", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Synonym", table_name = "Synonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Virus", table_name = "Virus", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000)
  ))
  return(defaults)
}

prompt_user_input <- function(){
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
  return(table_defaults)
  }

get_polis_data <- function(folder = NULL,
                           token = "",
                           verbose=TRUE,
                           dev = FALSE){
  

  #If folder location and token not provided, prompt user for input:
  
  if (is.null(folder) && interactive() && .Platform$OS.type == "windows"){
    folder <- utils::choose.dir(default = getwd(), caption = "Select a folder where POLIS data will be saved:")
  }
  
  #If folder structure and cache not created already, create them
  init_polis_data_struc(folder=folder, token=token)
    
  #If token not provided, check for it in cache. 
    if(load_specs()$polis$token == ""){
      # If not in cache, if the token was provided as a function parameter, use that, else prompt for token entry
        valid_token <- TRUE
        if(token == ""){
          token <- readline("Enter POLIS APIv2 Token: ")
        }  
        #Check if token is valid, stop if not
        valid_token <- validate_token(token)
        if(valid_token == FALSE){
          stop("Invalid token.")
        }
    #save new token to specs yaml
      specs_yaml <- file.path(folder,'cache_dir','specs.yaml')
      specs <- read_yaml(file.path(folder,'cache_dir','specs.yaml'))
      specs$polis$token <- token
      write_yaml(specs, specs_yaml)
      Sys.setenv("token" = token)
    }
  
  #Get default POLIS table names, field names, and download sizes
  if(dev == TRUE){
  defaults <- load_defaults() %>%
    filter(grepl("RefData", table_name) | table_name == "Lqas") #Note: This filter is in place for development purposes - to reduce the time needed for testing. Remove for final
  }
  if(dev == FALSE){
    defaults <- load_defaults()
  }
  
  #run get_polis_table iteratively over all tables
  
  for(i in 1:nrow(defaults)){
    table_name <- defaults$table_name[i]
    field_name <- defaults$field_name[i]
    id_vars <- defaults$id_vars[i]
    download_size <- as.numeric(defaults$download_size[i])
    table_name_descriptive <- defaults$table_name_descriptive[i]
    print(paste0("Downloading ", table_name_descriptive," Table [", i,"/", nrow(defaults),"]"))
    get_polis_table(folder = load_specs()$polis_data_folder,
                    token = load_specs()$polis$token,
                    table_name = table_name,
                    field_name = field_name,
                    id_vars = id_vars,
                    download_size = download_size,
                    table_name_descriptive = table_name_descriptive)
  }
  
  cat(paste0("POLIS data have been downloaded/updated and are stored locally at ", folder, ".\n\nTo load all POLIS data please run load_raw_polis_data().\n\nTo review meta data about the cache run [load cache data function]\n"))
}

#Run a simple API call to check if the user-provided token is valid
validate_token <- function(token = token){
  my_url_test <-  paste0('https://extranet.who.int/polis/api/v2/',
                         'Case?',
                     "$inlinecount=allpages&$top=0",
                     '&token=',token) %>%
                  httr::modify_url()
  result_test <- httr::GET(my_url_test)$status
  valid_token <- TRUE
  if(result_test != 200){
    valid_token <- FALSE
  }
  return(valid_token)
}

#Function that moves the rds files in the polis_data folder to an archive folder
archive_last_data <- function(table_name,
                              archive_folder = NULL, #folder pathway where the datasets will be archived
                              n_archive = 3 #Number of most-recent datasets to save in archive, per table
){
  #If archive_folder was not specified, then check if the default exists, if not then create it
  if(is.null(archive_folder)){
    archive_folder = paste0(load_specs()$polis_data_folder,"\\archive")
    if(file.exists(archive_folder) == FALSE){
      dir.create(archive_folder)
    }
  }
  
  #Get list of rds files to archive from polis_data_folder
  # current_files <- list.files(load_specs()$polis_data_folder) %>%
  #   stringr::str_subset(., pattern=".rds") %>%
  #   stringr::str_remove(., pattern=".rds")
  
  current_files <- table_name #Revised to just the current file (i.e. table name) so that the archive function can be placed in get_polis_table() instead of get_polis_data()
  
  #for each item in current_files list, check if an archive subfolder exists, and if not then create it
  # for(i in current_files){  
  
  if(file.exists(paste0(archive_folder, "\\", table_name)) == FALSE){
    dir.create(paste0(archive_folder, "\\", table_name))
  }
  #for each item in current_files:
  #delete the oldest file in it's subfolder if there are >= n_archive files in the subfolder
  archive_list <- list.files(paste0(archive_folder, "\\", table_name)) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  
  #if archive_list is not empty then get timestamps
  if(length(archive_list) > 0){
    archive_list_timestamp <- c()
    for(j in archive_list){
      timestamp <- as.POSIXct(file.info(paste0(archive_folder, "\\", table_name, "\\", j,".rds"))$ctime)
      archive_list_timestamp <- as.POSIXct(c(archive_list_timestamp, timestamp), origin=lubridate::origin)
    }
    oldest_file <- (bind_cols(file=archive_list, timestamp=archive_list_timestamp) %>%
                      mutate(timestamp   = as.POSIXct(timestamp)) %>%
                      arrange(timestamp) %>%
                      slice(1))$file %>%
      paste0(., ".rds")
  }
  if(length(archive_list) >= n_archive){
    file.remove(paste0(archive_folder, "\\", table_name, "\\", oldest_file))
  }
  
  #write the current file to the archive subfolder
  
  if(file.exists(paste0(load_specs()$polis_data_folder, "\\", table_name,".rds"))){
    current_file_timestamp <- attr(readRDS(paste0(load_specs()$polis_data_folder, "\\", table_name,".rds")), which="updated")
    current_file <- readRDS(paste0(load_specs()$polis_data_folder, "\\", table_name,".rds"))
    write_rds(current_file, paste0(archive_folder, "\\", table_name, "\\", table_name, "_", format(as.POSIXct(current_file_timestamp), "%Y%m%d_%H%M%S_"),".rds"))
    #remove the current file from the main folder #Undid this as the current file is needed for append_and_save()
    # file.remove(paste0(load_specs()$polis_data_folder, "\\", i,".rds"))
  }
 
}


#data cleaning: standardize variable names by the following rules: [From: https://cran.r-project.org/web/packages/janitor/vignettes/janitor.html]
  # Parse letter cases and separators to a consistent format (e.g. snake_case)
  # Remove leading/trailing/repeating spaces
  # Replace special characters with alphanumeric (e.g. o to oe)
  # Append numbers to duplicated names
  # Convert '%' to 'percent' and '#' to number

cleaning_var_names_initial <- function(input_dataframe = NULL,
                                       case="snake"){
  if(!is.null(input_dataframe)){
  input_dataframe <- input_dataframe %>%
    janitor::clean_names(case=case) %>%
    rename_all(stringi::stri_trans_general,
               id = "Latin-ASCII")
  }
  return(input_dataframe)
}

add_cache_attributes <- function(table_name){
  #Get list of rds 
  # current_files <- list.files(load_specs()$polis_data_folder) %>%
  #   stringr::str_subset(., pattern=".rds") %>%
  #   stringr::str_remove(., pattern=".rds") 
  
  #For each rds, read it in, assign attributes from cache, and save it
    #get cache entry
    cache_entry <- read_cache(.file_name = table_name)
    #read in file
    file <- readRDS(paste0(load_specs()$polis_data_folder,"\\", table_name,".rds"))
    #Assign attributes  
    attributes(file)$created <- cache_entry$created
    attributes(file)$date_field <- cache_entry$date_field
    attributes(file)$file_name <- cache_entry$file_name
    attributes(file)$file_type <- cache_entry$file_type
    attributes(file)$latest_date <- cache_entry$latest_date
    attributes(file)$updated <- cache_entry$updated
    #Write file
    write_rds(file, paste0(load_specs()$polis_data_folder,"\\", table_name,".rds"))
}

#data cleaning: re-assign classes to each variable by the following rules (applied in order): [From: https://r4ds.had.co.nz/data-import.html]
  # 1. logical: contains only "F", "T", "FALSE", or "TRUE"
  # 2. integer: contains only numeric characters (and -).
  # 3. double: contains only valid doubles (including numbers like 4.5e-5)
  # 4. number: contains valid doubles with the grouping mark inside.
  # 5. time: matches the default time_format
  # 6. date: matches the default date_format
  # 7. date-time: any ISO8601 date
  # 8. If none of the above apply, then keep col as character.
cleaning_var_class_initial <- function(input_dataframe = NULL){
  if(!is.null(input_dataframe)){
    #Get 3 sets of 1000 rows to use to guess classes and compare
      #if <1000 rows, the full dataset will be used to guess classes (no need to compare)
    if(nrow(input_dataframe) > 1000){
    set1 <- input_dataframe %>%
      #Sample 1000 rows at random 
      sample_n(size=1000, replace=FALSE)
    class_set1 <- set1 %>%
      mutate_all( ~ guess_parser(.)) %>%
      unique() 
      
    set2 <- input_dataframe %>%
      head(1000)
    class_set2 <- set2 %>%
      mutate_all( ~ guess_parser(.)) %>%
      unique() 
    
    set3 <- input_dataframe %>%
      tail(1000)
    class_set3 <- set3 %>%
      mutate_all( ~ guess_parser(.)) %>%
      unique() 
    
    class_set <- class_set1 %>%
      bind_rows(class_set2) %>%
      bind_rows(class_set3) %>%
      unique()
    
    class_set_keep_character <- class_set %>%
      summarise_all(n_distinct) %>%
      t() %>%
      as.data.frame() %>%
      rename(number_of_class_guesses = V1) %>%
      filter(number_of_class_guesses > 1)
    
    class_set_keep_character$var_name <- row.names(class_set_keep_character)
    class_set <- class_set %>%
      head(1) %>%
      t() %>%
      as.data.frame() %>%
      rename(class = V1) 
    
    class_set$var_name <- row.names(class_set)
    class_set <- class_set %>% 
      mutate(class = ifelse(var_name %in% class_set_keep_character$var_name, "character", class))
    rownames(class_set) = NULL
    class_set <- class_set %>%
      pivot_wider(names_from = var_name, values_from=class)
    }
    if(nrow(input_dataframe <= 1000)){
      class_set <- input_dataframe %>%
        mutate_all( ~ guess_parser(.)) %>%
        unique() 
    }
    input_dataframe <- input_dataframe %>%
      mutate_at(colnames(class_set), parse_guess) 
    
    #Convert POSIXct to date when no time info is available
      posixct_dates <- input_dataframe %>%
        select_if(is.POSIXct) %>%
        mutate_all(., ~ifelse((strftime(as.POSIXlt(.), format="%H:%M:%S"))=="00:00:00", 0,1)) %>%
        summarise_all(max, na.rm=TRUE) %>%
        t() %>%
        as.data.frame() %>%
        rename(date_and_time = V1) %>%
        filter(date_and_time == 0) %>%
        t() %>%
        colnames()
      input_dataframe <- input_dataframe %>%
        mutate_at(posixct_dates, as.Date)
  }
  return(input_dataframe)
}

#data cleaning: remove duplicates either across all vars, or a set of vars if specified by the user
cleaning_dedup <- function(input_dataframe = NULL,
                          dedup_vars = NULL #a vector of var names to deduplicate on
                          ){
  if(!is.null(input_dataframe)){
      if(is.null(dedup_vars)){
        input_dataframe <- input_dataframe %>%
          distinct(across(everything()))
      }
    if(!is.null(dedup_vars)){
      input_dataframe <- input_dataframe %>%
        distinct(across(as.vector(dedup_vars)))
    }
  }
  return(input_dataframe)
}

#data cleaning: remove empty rows and columns
cleaning_remove_empty_rows <- function(input_dataframe = NULL){
  if(!is.null(input_dataframe)){
    input_dataframe <- input_dataframe %>%
      janitor::remove_empty(which=c("rows"))
  }
  return(input_dataframe)
}


#data cleaning: rename from file
cleaning_var_names_from_file <- function(table_name = NULL,
                                         input_dataframe = NULL,
                                         var_name_file = NULL,
                                         desired_naming_convention = NULL){
  if(is.null(var_name_file)){
    #Check if file exists. if not, then create the folder and download the file
    if(file.exists(paste0(load_specs()$polis_data_folder, "/datafiles/var_name_synonyms.rds")) == FALSE){
      if(file.exists(paste0(load_specs()$polis_data_folder, "/datafiles")) == FALSE){
        dir.create(paste0(load_specs()$polis_data_folder, "/datafiles"))
      }
      var_name_synonyms <- read.csv("https://raw.githubusercontent.com/nish-kishore/polis_api_etl/lbaertlein1-patch-1/datafiles/var_name_synonyms.rds?token=GHSAT0AAAAAABUCSLUVJW7PMDMWI37IARHYYVA4CHA")
      write_rds(var_name_synonyms, paste0(load_specs()$polis_data_folder, "/datafiles/var_name_synonyms.rds"))
    }
    var_name_synonyms <- readRDS(paste0(load_specs()$polis_data_folder, "/datafiles/var_name_synonyms.rds"))
  }
  if(!is.null(var_name_file)){
    var_name_synonyms <- readRDS(var_name_file)
  }

  
  table_var_name_synonyms <- var_name_synonyms %>%
    filter(table_name == table_name) %>%
    janitor::remove_empty(which=c("rows", "cols")) %>%
    janitor::clean_names() %>%
    select(-c("table_name"))
  
  if(is.null(desired_naming_convention)){
    desired_naming_convention <- colnames(table_var_name_synonyms)[utils::menu(colnames(table_var_name_synonyms), title="Select a Variable Naming Convention:")]
  }
  original_to_desired <- var_name_synonyms %>%
    select(original_var_name, {{desired_naming_convention}} )
  for(i in 1:ncol(input_dataframe)){
    original_name <- colnames(input_dataframe)[i]
    new_name <- (original_to_desired %>%
      filter(original_var_name == {{original_name}}))[1,2]
    input_dataframe <- input_dataframe %>%
      rename({{new_name}} := {{original_name}})
  }
  return(input_dataframe)
}

compare_final_to_archive <- function(table_name,
                                     id_vars,
                                     categorical_max = 30){
  id_vars <- as.vector(id_vars)
  
  #if archive exists, then compare final to archive. If not, go to end;
  if(file.exists(paste0(load_specs()$polis_data_folder,"\\archive\\", table_name)) &
     length(list.files(paste0(paste0(load_specs()$polis_data_folder,"\\archive\\", table_name)))) > 0){
    #Load new_file
    new_file <- readRDS(paste0(load_specs()$polis_data_folder, "/", table_name, ".rds"))
    
    #load latest file in archive subfolder
    archive_subfolder <- paste0(load_specs()$polis_data_folder,"\\archive\\", table_name)
    
    #for each item in subfolder_list, get all file names then subset to most recent
    subfolder_files <- list.files(paste0(archive_subfolder))
    file_dates <- c()
    for(j in subfolder_files){
      file_date <- attr(readRDS(paste0(archive_subfolder, "\\", j)),which="updated")
      file_dates <- c(file_dates, file_date)
    }
    latest_file <- c()
    if(length(file_dates) > 0){
    latest_file <- (bind_cols(name = subfolder_files, create_date = file_dates) %>%
                      mutate(create_date = as.POSIXct(create_date, origin = lubridate::origin)) %>%
                      arrange(desc(create_date)) %>%
                      slice(1))$name
    }
    change_summary <- NULL
    if(length(latest_file) > 0){
      #load latest_file
      latest_file <- readRDS(paste0(archive_subfolder, "\\", latest_file))
      
      #get metadata for latest file and new_file
      new_file_metadata <- get_polis_metadata(query_output = new_file,
                                              table_name = table_name,
                                              categorical_max = categorical_max)
      old_file_metadata <- get_polis_metadata(query_output = latest_file,
                                              table_name = table_name,
                                              categorical_max = categorical_max)
      
      change_summary <- metadata_comparison(new_file_metadata, old_file_metadata)[2:5]
      
      #count obs added to new_file and get set
      in_new_not_old <- new_file %>%
        anti_join(latest_file, by=as.vector(id_vars))
      
      #count obs removed from old_file and get set
      in_old_not_new <- latest_file %>%
        anti_join(new_file, by=as.vector(id_vars))
      
      #count obs modified in new file compared to old and get set
      in_new_and_old_but_modified <- new_file %>%
        ungroup() %>%
        inner_join(latest_file %>% ungroup(), by=as.vector(id_vars)) %>%
        #restrict to cols in new and old
        select(id_vars, paste0(colnames(new_file %>% select(-id_vars)), ".x"), paste0(colnames(new_file %>% select(-id_vars)), ".y")) %>%
        #wide_to_long
        pivot_longer(cols=-id_vars) %>%
        mutate(source = ifelse(str_sub(name, -2) == ".x", "new", "old")) %>%
        mutate(name = str_sub(name, 1, -3)) %>%
        #long_to_wide
        pivot_wider(names_from=source, values_from=value, values_fn=list) %>%
        mutate(new = paste(new, collapse=", "),
               old = paste(old, collapse=", ")) %>%
        filter(new != old)
      
      #summary counts
      n_added <- nrow(in_new_not_old)
      n_edited <- nrow(in_new_and_old_but_modified %>%
                         select(id_vars) %>%
                         unique())
      n_deleted <- nrow(in_old_not_new)
      obs_change <- c(n_added = n_added,
                      n_edited = n_edited, 
                      n_deleted = n_deleted)
      
      #add summary to change_summary along with datasets
      change_summary <- append(change_summary, 
                               list(obs_change = obs_change, 
                                    obs_added = in_new_not_old,
                                    obs_edited = in_new_and_old_but_modified,
                                    obs_deleted = in_old_not_new))
      
    }
    return(change_summary)
  }
  #If archived file does not exist, then return NULL change_summary
  if(file.exists(paste0(load_specs()$polis_data_folder,"\\archive\\", table_name)) == FALSE){
    return(NULL)
  }
}


save_change_summary <- function(table_name, 
                                change_summary = NULL,
                                change_log_folder = NULL,
                                n_change_log = 30){
  #If change_summary is null, then go to end
  if(!is.null(change_summary)){
    #If change_log_folder was not specified, then check if the default exists, if not then create it
    if(is.null(change_log_folder)){
      change_log_folder = paste0(load_specs()$polis_data_folder,"\\change_log")
      if(file.exists(change_log_folder) == FALSE){
        dir.create(change_log_folder)
      }
    }
    
    #If change_log subfolder does not exist, then create it
    change_log_subfolder = paste0(load_specs()$polis_data_folder,"\\change_log\\", table_name)
    if(file.exists(change_log_subfolder) == FALSE){
      dir.create(change_log_subfolder)
    }
    
    #delete the oldest file in subfolder if there are >= n_change_log files in the subfolder
    change_log_list <- list.files(change_log_subfolder) %>%
      stringr::str_subset(., pattern=".rds") %>%
      stringr::str_remove(., pattern=".rds")
    change_log_list_timestamp <- c()
    for(j in change_log_list){
      timestamp <- as.POSIXct(file.info(paste0(change_log_subfolder, "\\", j,".rds"))$ctime)
      change_log_list_timestamp <- as.POSIXct(c(change_log_list_timestamp, timestamp), origin=lubridate::origin)
    }
    if(length(change_log_list) > 0){
    oldest_file <- (bind_cols(file=change_log_list, timestamp=change_log_list_timestamp) %>%
                      mutate(timestamp   = as.POSIXct(timestamp)) %>%
                      arrange(timestamp) %>%
                      slice(1))$file %>%
      paste0(., ".rds")
    }
    if(length(change_log_list) >= n_change_log){
      file.remove(paste0(change_log_subfolder, "\\", oldest_file))
    }
    #write the current file to the archive subfolder
    write_rds(change_summary, paste0(change_log_subfolder, "\\", table_name, "_change_log_", format(as.POSIXct(Sys.time()), "%Y%m%d_%H%M%S_"),".rds"))
  }

}

cleaning_var_class_from_metadata <- function(){
  url <- 'https://extranet.who.int/polis/api/v2/$metadata'
  
  nodes <- read_html(url, xpath = '//h3 | //*[contains(concat( " ", @class, " 
" ), concat( " ", "entry-title", " " ))]')
  
  page <- htmlTreeParse(nodes)$children[["html"]][["body"]][["edmx"]][["dataservices"]][["schema"]] %>%
    xmlToList(., simplify=TRUE, addAttributes = TRUE)
  
  metadata <- data.frame()
  for(i in 1:(length(page)-1)){
    table <- page[[i]][[".attrs"]][[1]]
    for(j in 2:(length(page[[i]])-1)){
      polis_name <- page[[i]][[j]][["name"]]
      polis_type <- page[[i]][[j]][["type"]]
      row <- c(table, polis_name, polis_type)
      metadata <- rbind(metadata, row) %>%
        rename(table = 1, polis_name = 2, polis_type = 3)
      print(paste0(i, ", ", j))
    }
  }  
  metadata <- metadata %>%
    mutate(table = str_replace_all(table, "ApiV2", ""),
           polis_type = str_replace_all(polis_type, "Edm.", ""),
           class = case_when(polis_type %in% c("Boolean") ~ "logical",
                             polis_type %in% c("DateTime") ~ "POSIXct",
                             polis_type %in% c("Decimal", "Double", "Int16", "Int32", "Int64") ~ "numeric",
                             polis_type %in% c("Guid", "String") ~ "character",
                             TRUE ~ NA_character_))
  return(metadata)
}

#data cleaning: convert all empty strings to NA
cleaning_blank_to_na <- function(input_dataframe){
  input_dataframe <- input_dataframe %>%
    mutate_all(list(~str_trim(.))) %>% #remove all leading/trailing whitespaces as well as replace all " " with ""
    mutate_all(list(~na_if(.,""))) #replace "" with NA
}

#data cleaning: replace all special characters with alphanumeric
cleaning_replace_special <- function(input_dataframe){
  #for each character variable, replace special characters
  input_dataframe <- input_dataframe %>%
    mutate_if(is.character, 
              stringi::stri_trans_general,
              id = "Latin-ASCII") 
  return(input_dataframe)
}

clean_polis_data <- function(input_dataframe = NULL){
  input_dataframe <- cleaning_var_names_initial(input_dataframe)
  input_dataframe <- cleaning_var_class_initial(input_dataframe)
  input_dataframe <- cleaning_remove_empty_rows(input_dataframe)
  input_dataframe <- cleaning_blank_to_na(input_dataframe)
  input_dataframe <- cleaning_replace_special(input_dataframe)
  input_dataframe <- cleaning_dedup(input_dataframe)
  input_dataframe <- cleaning_var_class_initial(input_dataframe) #running cleaning_var_class_initial a second time converts character to logical 
  return(input_dataframe)
}

create_url_array_idvars_and_field_name <- function(table_name = table_name,
                                                   id_vars = id_vars,
                                                   field_name = field_name,
                                                   min_date = min_date){
  # construct general URL
  filter_url_conv <- paste0("(",
         date_min_conv(field_name, min_date),
         ") or (",
         date_null_conv(field_name), ")")
    
  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=",
                   if(filter_url_conv == "") "" else paste0(filter_url_conv),
                   "&$select=", paste0(paste(id_vars, collapse=","), ", ", field_name),
                   # "&select=", paste0(paste(id_vars, collapse=","), ", ", field_name),
                   # "&select=", paste0(paste(id_vars, collapse=",")),
                   "&$inlinecount=allpages",
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  # Get table size
    my_url2 <- paste0('https://extranet.who.int/polis/api/v2/',
                    paste0(table_name, "?"),
                    "$inlinecount=allpages",
                    '&token=',load_specs()$polis$token,
                    "&$top=0") %>%
    httr::modify_url()
  
  response <- httr::GET(my_url2)
  
  table_size <- response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer()
  # build URL array
  urls <- paste0(my_url, "&$top=", as.numeric(1000), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(1000)))
  return(urls)
}

create_url_array_id_section <- function(table_name = table_name,
                                        id_section_table = id_section_table){
  urls <- c()
  for(i in id_section_table$filter_url_conv){
    url <- paste0('https://extranet.who.int/polis/api/v2/',
                  paste0(table_name, "?"),
                  '$filter=', 
                  i,
                  '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
    urls <- c(urls, url)
  }
  return(urls)
}
create_url_array_id_method <- function(table_name,
                            id_vars,
                            field_name,
                            min_date = min_date){
  get_ids_for_url_array(table_name, id_vars, field_name, min_date)
  id_list <- readRDS(paste0(load_specs()$polis_data_folder,"/id_list_temporary_file.rds"))
  id_list2 <- id_list %>% 
    select(Id) %>%
    unique() %>%
    mutate(Id = as.numeric(Id)) %>%
    arrange(Id) %>%
    mutate(is_lag_sequential_fwd = ifelse(lag(Id) == Id -1, 1, 0)) %>%
    mutate(is_lag_sequential_fwd = ifelse(is.na(is_lag_sequential_fwd), 0, is_lag_sequential_fwd)) %>%
    arrange(desc(Id)) %>%
    mutate(is_lag_sequential_rwd = ifelse(lag(Id) == Id + 1, 1, 0)) %>%
    mutate(is_lag_sequential_rwd = ifelse(is.na(is_lag_sequential_rwd), 0, is_lag_sequential_rwd)) %>%
    rowwise() %>%
    mutate(flag = sum(is_lag_sequential_fwd, is_lag_sequential_rwd)) %>%
    mutate(flag = ifelse(is.na(flag), 0, flag)) %>%
    ungroup() 
  individual_ids <- id_list2 %>%
    filter(flag==0) %>%
    mutate(start = Id, end = Id) %>%
    select(start, end)

  id_grps <- id_list2 %>%    
    filter(flag == 1) %>%
    select(Id) %>%
    arrange(Id) %>%
    mutate(x = ifelse(((row_number() %% 2) == 0), "end", "start")) %>%
    mutate(grp = ifelse(x == "start", row_number(), lag(row_number()))) %>%
    pivot_wider(id_cols= grp, names_from = x, values_from=Id) %>%
    select(-grp) %>%
    rbind(individual_ids) %>%
    unique()
  
  for(i in 1:nrow(id_grps)){
    min_id <- id_grps$start[i]
    max_id <- id_grps$end[i]
    break_list_start <- seq(min_id, max_id, by=1000)
    break_list_end <- seq(min_id+999, max_id+999, by=1000)
    if(i == 1){
    id_section_table <- data.frame(start=break_list_start, end=break_list_end, min=min_id, max=max_id) %>%
      mutate_all(as.numeric) %>%
      rowwise() %>%
      mutate(end = ifelse(end > max, max, end)) %>%
      ungroup() %>%
      mutate(filter_url_conv = paste0("((Id ge ", start, ") and (Id le ", end,"))"))
    }
    if(i != 1){
      id_section_table <- id_section_table %>%
        bind_rows(data.frame(start=break_list_start, end=break_list_end, min=min_id, max=max_id) %>%
                    mutate_all(as.numeric) %>%
                    rowwise() %>%
                    mutate(end = ifelse(end > max, max, end)) %>%
                    ungroup() %>%
                    mutate(filter_url_conv = paste0("((Id ge ", start, ") and (Id le ", end,"))")))
    }
  }  
  #create list of seq first to last by 1000
    urls <- create_url_array_id_section(table_name, id_section_table)
  return(urls)
}

check_if_id_exists <- function(table_name,
                               id_vars = "Id"){
  url <-  paste0('https://extranet.who.int/polis/api/v2/',
                        paste0(table_name, "?"),
                        '$select=', 
                        paste(id_vars, collapse=", "),
                        '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  status <- as.character(httr::GET(url)$status_code)
  id_exists <- ifelse(status=="200", TRUE, FALSE)
  return(id_exists)
}
get_ids_for_url_array <- function(table_name,
                                  id_vars,
                                  field_name,
                                  min_date = min_date){
  urls <- create_url_array_idvars_and_field_name(table_name, id_vars, field_name, min_date)
  id_list <- pb_mc_api_pull(urls)
  write_rds(id_list, paste0(load_specs()$polis_data_folder,"/id_list_temporary_file.rds"))
}

#Function that moves the rds files in the polis_data folder to an snapshot folder
save_snapshot <- function(snapshot_folder = NULL, #folder pathway where the datasets will be saved
                          snapshot_date = Sys.time()
){
  #If snapshot_folder was not specified, then check if the default exists, if not then create it
  if(is.null(snapshot_folder)){
    snapshot_folder = paste0(load_specs()$polis_data_folder,"\\snapshots")
    if(file.exists(snapshot_folder) == FALSE){
      dir.create(snapshot_folder)
    }
  }
  
  #Create snapshot subfolder
  x <- gsub(":","", snapshot_date)
  x <- gsub(" ","_", x)
  x <- gsub("-","", x)
  snapshot_subfolder <- paste0(snapshot_folder, "\\snapshot_", x)
  dir.create(snapshot_subfolder)
  #Get list of rds files to save in snapshot from polis_data_folder
  current_files <- list.files(load_specs()$polis_data_folder) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  
  #for each item in current_files:
  
  #write the current file to the snapshot subfolder:
  for(i in current_files){
    write_rds(readRDS(paste0(load_specs()$polis_data_folder, "\\", i,".rds")),
              paste0(snapshot_subfolder, "\\", i,".rds"))
  }
}


revert_from_archive <- function(last_good_date = Sys.Date()-1){
  folder <- load_specs()$polis_data_folder
  archive_folder <- paste0(load_specs()$polis_data_folder,"\\archive")
  
  #for each subfolder of archive_folder, get the file name/path of the most recent file created on/before last_good_date
    #get directory of subfolders
      subfolder_list <- list.files(archive_folder)
    #for each item in subfolder_list, get all file names then subset to most recent
      for(i in subfolder_list){
        subfolder_files <- list.files(paste0(archive_folder, "\\", i))
        file_dates <- c()
        for(j in subfolder_files){
          file_date <- attr(readRDS(paste0(archive_folder, "\\", i, "\\", j)),which="updated")
          file_dates <- c(file_dates, file_date)
        }
        file_to_keep <- (bind_cols(name = subfolder_files, create_date = file_dates) %>%
          mutate(create_date = as.POSIXct(create_date, origin = lubridate::origin)) %>%
          filter(create_date <= as.POSIXct(paste0(last_good_date, " 23:59:59"), format="%Y-%m-%d %H:%M:%S", origin = lubridate::origin)) %>%
          arrange(desc(create_date)) %>%
          slice(1))$name
        #load file to keep
        if(length(file_to_keep) > 0){
        file_to_keep <- readRDS(paste0(archive_folder, "\\", i, "\\", file_to_keep))
        #write file to keep to data folder
        write_rds(file_to_keep, paste0(folder,"\\",i,".rds"))
        }
        if(length(file_to_keep) == 0){
          warning(paste0("There is no ", i, " table with an acceptable date in the archive. Current file retained."))
        }
      }
  #Update the cache to reflect the reverted files
      update_cache_from_files()
}

update_cache_from_files <- function(){
  #read in cache
  cache <- readRDS(file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')) 

  #update the created, updated, and latest dates in the cache
    #get list of rds files  
      current_files <- list.files(load_specs()$polis_data_folder) %>%
        stringr::str_subset(., pattern=".rds") %>%
        stringr::str_remove(., pattern=".rds")
    #For each rds, read its attributes, assign attributes to cache
      for(i in current_files){  
        #read attributes  
        attr_created <- attr(readRDS(paste0(load_specs()$polis_data_folder,"\\",i,".rds")), which = "created") 
        attr_date_field <- attr(readRDS(paste0(load_specs()$polis_data_folder,"\\",i,".rds")), which = "date_field") 
        attr_file_type <- attr(readRDS(paste0(load_specs()$polis_data_folder,"\\",i,".rds")), which = "file_type") 
        attr_latest_date <- attr(readRDS(paste0(load_specs()$polis_data_folder,"\\",i,".rds")), which = "latest_date") 
        attr_updated <- attr(readRDS(paste0(load_specs()$polis_data_folder,"\\",i,".rds")), which = "updated") 
        cache <- cache %>%
          mutate(created = as.POSIXct(ifelse(file_name == i, attr_created, created), format=("%Y-%m-%d %H:%M:%S"), origin = lubridate::origin),
                 date_field = ifelse(file_name == i, attr_date_field, date_field),
                 file_type = ifelse(file_name == i, attr_file_type, file_type),
                 latest_date = as.Date(ifelse(file_name == i, attr_latest_date, latest_date), format=("%Y-%m-%d"), origin=lubridate::origin),
                 updated = as.POSIXct(ifelse(file_name == i, attr_updated, updated), format=("%Y-%m-%d %H:%M:%S"), origin = lubridate::origin))
      }
      #Write revised cache
      write_rds(cache, file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds'))
}

#for any table, pull just the idvars for a check for deletions in POLIS and a
# a double-check for any additions in POLIS not captured by the date_field query

create_url_array_idvars <- function(table_name = table_name,
                                    id_vars = id_vars){
  # construct general URL
    my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$select=", paste(id_vars, collapse=","),
                   "&$inlinecount=allpages",
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  # Get table size
      my_url2 <- paste0('https://extranet.who.int/polis/api/v2/',
                         paste0(table_name, "?"),
                         "$inlinecount=allpages",
                         '&token=',load_specs()$polis$token,
                         "&$top=0") %>%
          httr::modify_url()
    
     response <- httr::GET(my_url2)
    
     table_size <- response %>%
                    httr::content(type='text',encoding = 'UTF-8') %>%
                    jsonlite::fromJSON() %>%
                    {.$odata.count} %>%
                    as.integer()
  # build URL array
    urls <- paste0(my_url, "&$top=", as.numeric(1000), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(1000)))
    return(urls)
}

get_idvars_only <- function(table_name,
                            id_vars){
  urls <- create_url_array_idvars(table_name, id_vars)
  query_start_time <- Sys.time()
  query_output <- pb_mc_api_pull(urls)  
  query_stop_time <- Sys.time()
  query_time <- round(difftime(query_stop_time, query_start_time, units="auto"),0)
  # print(paste0("Pulled all IdVars for ", table_name, " (", nrow(query_output), " rows in ", query_time, " ", attr(query_time, which="units"),")"))
  return(query_output)
}

#function to remove the obs deleted from the POLIS table from the saved table
find_and_remove_deleted_obs <- function(full_idvars_output,
                                  new_complete_file,
                                  id_vars){
  id_vars <- as.vector(id_vars)
  new_complete_file_idvars <- new_complete_file %>%
    select(id_vars)
  deleted_obs <- new_complete_file_idvars %>%
    anti_join(full_idvars_output, by=id_vars)
  new_complete_file <- new_complete_file %>%
    anti_join(deleted_obs, by=id_vars)
  return(new_complete_file)
}

compare_final_to_archive <- function(table_name,
                                     id_vars,
                                     categorical_max = 30){
  id_vars <- as.vector(id_vars)
  #Load new_file
  new_file <- readRDS(paste0(load_specs()$polis_data_folder, "/", table_name, ".rds"))

  #load latest file in archive subfolder
  archive_subfolder <- paste0(load_specs()$polis_data_folder,"\\archive\\", table_name)

  #for each item in subfolder_list, get all file names then subset to most recent
    subfolder_files <- list.files(paste0(archive_subfolder))
    file_dates <- c()
    for(j in subfolder_files){
      file_date <- attr(readRDS(paste0(archive_subfolder, "\\", j)),which="updated")
      file_dates <- c(file_dates, file_date)
    }
    latest_file <- c()
    if(length(subfolder_files)>0){
    latest_file <- (bind_cols(name = subfolder_files, create_date = file_dates) %>%
                       mutate(create_date = as.POSIXct(create_date, origin = lubridate::origin)) %>%
                       arrange(desc(create_date)) %>%
                       slice(1))$name
    }
    change_summary <- NULL
    if(length(latest_file) > 0){
      #load latest_file
      latest_file <- readRDS(paste0(archive_subfolder, "\\", latest_file))

      #get metadata for latest file and new_file
      new_file_metadata <- get_polis_metadata(query_output = new_file,
                         table_name = table_name,
                         categorical_max = categorical_max)
      old_file_metadata <- get_polis_metadata(query_output = latest_file,
                                              table_name = table_name,
                                              categorical_max = categorical_max)

      change_summary <- metadata_comparison(new_file_metadata, old_file_metadata)[2:5]

      #count obs added to new_file and get set
      in_new_not_old <- new_file %>%
        anti_join(latest_file, by=as.vector(id_vars))

      #count obs removed from old_file and get set
      in_old_not_new <- latest_file %>%
        anti_join(new_file, by=as.vector(id_vars))

      #count obs modified in new file compared to old and get set
        in_new_and_old_but_modified <- new_file %>%
          inner_join(latest_file, by=as.vector(id_vars)) %>%
          #restrict to cols in new and old
          select(id_vars, paste0(colnames(new_file %>% select(-id_vars)), ".x"), paste0(colnames(new_file %>% select(-id_vars)), ".y")) %>%
          #wide_to_long
          pivot_longer(cols=-id_vars) %>%
          mutate(source = ifelse(str_sub(name, -2) == ".x", "new", "old")) %>%
          mutate(name = str_sub(name, 1, -3)) %>%
          #long_to_wide
          pivot_wider(names_from=source, values_from=value) %>%
          filter(new != old)
      
      #summary counts
        n_added <- nrow(in_new_not_old)
        n_edited <- nrow(in_new_and_old_but_modified %>%
                      select(id_vars) %>%
                      unique())
        n_deleted <- nrow(in_old_not_new)
        obs_change <- c(n_added = n_added,
                        n_edited = n_edited, 
                        n_deleted = n_deleted)
        
      #add summary to change_summary along with datasets
      change_summary <- append(change_summary, 
                             list(obs_change = obs_change, 
                                  obs_added = in_new_not_old,
                                  obs_edited = in_new_and_old_but_modified,
                                  obs_deleted = in_old_not_new))
          
      }
    return(change_summary)
}

save_change_summary <- function(table_name, 
                                change_summary,
                                change_log_folder = NULL,
                                n_change_log = 30){
  #If change_log_folder was not specified, then check if the default exists, if not then create it
  if(is.null(change_log_folder)){
    change_log_folder = paste0(load_specs()$polis_data_folder,"\\change_log")
    if(file.exists(change_log_folder) == FALSE){
      dir.create(change_log_folder)
    }
  }
  
  #If change_log subfolder does not exist, then create it
  change_log_subfolder = paste0(load_specs()$polis_data_folder,"\\change_log\\", table_name)
  if(file.exists(change_log_subfolder) == FALSE){
    dir.create(change_log_subfolder)
  }
  
  #delete the oldest file in subfolder if there are >= n_change_log files in the subfolder
  change_log_list <- list.files(change_log_subfolder) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  change_log_list_timestamp <- c()
  for(j in change_log_list){
    timestamp <- as.POSIXct(file.info(paste0(change_log_subfolder, "\\", j,".rds"))$ctime)
    change_log_list_timestamp <- as.POSIXct(c(change_log_list_timestamp, timestamp), origin=lubridate::origin)
  }
  if(length(change_log_list) > 0){
  oldest_file <- (bind_cols(file=change_log_list, timestamp=change_log_list_timestamp) %>%
                    mutate(timestamp   = as.POSIXct(timestamp)) %>%
                    arrange(timestamp) %>%
                    slice(1))$file %>%
    paste0(., ".rds")
  }
  if(length(change_log_list) >= n_change_log){
    file.remove(paste0(change_log_subfolder, "\\", oldest_file))
  }
  #write the current file to the archive subfolder
  write_rds(change_summary, paste0(change_log_subfolder, "\\", table_name, "_change_log_", format(as.POSIXct(Sys.time()), "%Y%m%d_%H%M%S_"),".rds"))
}

#function which prints the latest change log into console
print_latest_change_log_summary <- function(){
  #load and combine latest change_log for all tables
    #Check if change_log folder exists. If not, go to end.
    change_log_folder <- paste0(load_specs()$polis_data_folder,"\\change_log")
    if(file.exists(change_log_folder) == TRUE){
      #Get list of subfolders
      change_log_subfolder_list <- list.files(change_log_folder)
      #Get list of latest file name within each change_log_subfolder
      change_log_file_list <- c()
      obs_change_combined <- data.frame()
      class_changed_vars_combined <- data.frame()
      new_response_combined <- data.frame()
      lost_vars_combined <- data.frame()
      new_vars_combined <- data.frame()
      for(i in change_log_subfolder_list){
        change_log_list <- c()
        change_log_subfolder <- paste0(change_log_folder, "\\", i)
        change_log_list <- list.files(change_log_subfolder) %>%
          stringr::str_subset(., pattern=".rds") %>%
          stringr::str_remove(., pattern=".rds")
        change_log_list_timestamp <- c()  
        for(j in change_log_list){
          timestamp <- as.POSIXct(file.info(paste0(change_log_subfolder, "\\", j,".rds"))$ctime)
          change_log_list_timestamp <- as.POSIXct(c(change_log_list_timestamp, timestamp), origin=lubridate::origin)
        }
        if(length(change_log_list) > 0){
          newest_file <- (bind_cols(file=change_log_list, timestamp=change_log_list_timestamp) %>%
                          mutate(timestamp   = as.POSIXct(timestamp)) %>%
                          arrange(desc(timestamp)) %>%
                          slice(1))$file %>%
          paste0(., ".rds")
          change_log <- readRDS(paste0(change_log_subfolder, "\\", newest_file))
          new_response <- change_log$new_response %>%
            mutate(table_name = i)
          class_changed_vars <- change_log$class_changed_vars %>%
            mutate(table_name = i)
          if(length(change_log$lost_vars) > 0){
          lost_vars <- c(i, paste(change_log$lost_vars, ","))
          }
          if(length(change_log$lost_vars) == 0){
            lost_vars <- c()
          }
          if(length(change_log$lost_vars) > 0){
            new_vars <- c(i, paste(change_log$new_vars, ","))
          }
          if(length(change_log$new_vars) == 0){
            new_vars <- c()
          }
          obs_change <- as.data.frame(change_log$obs_change) 
          obs_change$change <- rownames(obs_change)
          obs_change <- obs_change %>%
            pivot_wider(names_from = change, values_from=`change_log$obs_change`) %>%
            mutate(table_name = i)
          
          obs_change_combined <- obs_change_combined %>%
            bind_rows(obs_change)
          class_changed_vars_combined <- class_changed_vars_combined %>%
            bind_rows(class_changed_vars)
          new_response_combined <- new_response_combined %>%
            bind_rows(new_response)
          lost_vars_combined <- lost_vars_combined %>%
            rbind(lost_vars)
          if(ncol(lost_vars_combined) == 2){colnames(lost_vars_combined) <- c("table_name", "lost_vars")}
          new_vars_combined <- new_vars_combined %>%
            rbind(new_vars)
          if(ncol(new_vars_combined) == 2){colnames(new_vars_combined) <- c("table_name", "new_vars")}
        }
      }
      #for each type of change, filter to where there are changes if needed
      obs_change_combined %>%
        rowwise() %>%
        mutate(tot_change = sum(n_added, n_edited, n_deleted)) %>%
        ungroup() %>%
        filter(tot_change > 0) %>%
        select(table_name, n_added, n_edited, n_deleted)
      
      #print summary of each type of change 
      if(nrow(new_vars_combined) > 0){
        print("New variables were found in the following tables:")
        new_vars_combined
      } else {print("No new variables were found in any table since last download.")}
      
      if(nrow(lost_vars_combined) > 0){
        print("Variables were dropped from the following tables:")
        lost_vars_combined
      } else {print("No variables were dropped from any table since last download.")}
      
      if(nrow(new_response_combined) > 0){
        print("New categorical responses were found in the following tables/variables:")
        new_response_combined %>% select(table_name, var_name, in_new_not_old)
      } else {print("No new categorical responses were found in any table since last download.")}
      
      if(nrow(class_changed_vars_combined) > 0){
        print("Changes in variable classes were found in the following tables:")
        class_changed_vars_combined %>% select(table_name, var_name, old_var_class, new_var_class)
      } else {print("No changes in variable classes were found in any table since last download.")}
      
      if(nrow(obs_change_combined) > 0){
        print("The following counts of observations were added/edited/deleted from each table since the last download:")
        obs_change_combined
      } else {print("No observation additions/edits/deletions were found in any table since last download.")}
    }
}