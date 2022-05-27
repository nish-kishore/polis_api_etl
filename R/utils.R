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
                            table_name = table_name){

  #If the newly pulled dataset has any data, then read in the old file, remove rows from the old file that are in the new file, then bind the new file and old file
  if(!is.null(query_output) & nrow(query_output) > 0 & file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
  old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
                mutate_all(.,as.character) %>%
      #remove records that are in new file
      anti_join(query_output, by=id_vars)

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
    if(table_count2 == nrow(old_polis) + nrow(query_output)){
    new_query_output <- query_output %>%
      bind_rows(old_polis)
    #write to env
    assign(quo_name(enquo(table_name)), new_query_output, envir=.GlobalEnv)
    #save to file
    write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
    return(new_query_output)
    }

    #If the overall number of rows in the table is not equal to old and new combined, then stop and flag for investigation
      #NOTE: instead of flagging, this could just trigger a re-pull of the full dataset
      if(table_count2 != nrow(old_polis) + nrow(query_output)){
      stop("Table is incomplete: check id_vars and field_name")
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
    if(table_count2 == nrow(query_output)){
      new_query_output <- query_output 
      #write to env
      assign(quo_name(enquo(table_name)), new_query_output, envir=.GlobalEnv)
      #save to file
      write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
      return(new_query_output)
    }
    if(table_count2 != nrow(query_output)){
      stop("Table is incomplete: check id_vars and field_name")
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
    latest_date <<- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
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
    latest_date <<- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
    }
  #If field_name is blank or not in the dataset, save latest_date as NA
  if(!is.null(query_output) && nrow(query_output) > 0 && !(field_name %in% colnames(query_output))){
    latest_date <<- NA
  }
      #Save the current system time as the date/time of 'updated' - the date the database was last checked for updates
      updated <<- Sys.time()
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
      new_vars <<- new_vars
      warning(print("There are new variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'new_vars'"))
    }

    lost_vars <- (compare_metadata %>%
      filter(is.na(var_class.y)))$var_name

    if(length(lost_vars) != 0){
      lost_vars <<- lost_vars
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
      class_changed_vars <<- class_changed_vars
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
      new_response <<- new_response
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
  else{
    re_pull_polis_indicator <- FALSE
  }
  return(re_pull_polis_indicator)
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
                            download_size = NULL){
  
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
        field_name_change <<- FALSE        
        cache_dir <- file.path(folder, "cache_dir")
        cache_file <- file.path(cache_dir, "cache.rds")
        #if a row with the table_name exists within cache, then pull the values from that row
        if(nrow(read_cache(.file_name = table_name)) != 0){
          old_field_name <- (readRDS(cache_file) %>%
                           filter(file_name == table_name))$date_field
          field_name_change <<- field_name != old_field_name
        }
  
  #Create cache entry and blank dataframe for a POLIS data table if it does not already exist
  init_polis_data_table(table_name, field_name)

  #Read the cache entry for the requested POLIS data table
  x <- NULL
  x <- read_table_in_cache_dir(table_name)
  
  #Create an API URL and use it to query POLIS
  urls <- create_url_array(table_name = table_name,
                           field_name = x$field_name,
                           min_date = x$latest_date,
                           download_size = download_size)
  
  query_output <- pb_mc_api_pull(urls)  
  
  #If the query produced any output, summarise it's metadata
  new_table_metadata <- NULL
  if(!is.null(query_output) & nrow(query_output) != 0){
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
                        old_table_metadata = old_table_metadata)
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
    urls <- create_url_array(table_name = table_name,
                             field_name = x$field_name,
                             min_date = x$latest_date)
    
    query_output <- pb_mc_api_pull(urls)  
  }

  #Combine the query output with the old dataset and save
  new_query_output <- append_and_save(query_output = query_output,
                                      table_name = table_name,
                                      id_vars = id_vars)

  #Get the cache dates for the newly saved table
  get_update_cache_dates(query_output = new_query_output,
                         field_name = field_name,
                         table_name = table_name)

  #Update the cache date fields
  update_cache(.file_name = table_name,
               .val_to_update = "latest_date",
               .val = latest_date,
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  update_cache(.file_name = table_name,
               .val_to_update = "updated",
               .val = updated,
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  update_cache(.file_name = table_name,
               .val_to_update = "date_field",
               .val = field_name,
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
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

  urls <- paste0(my_url, "&$top=", download_size, "&$skip=",seq(0,table_size, by = download_size))
  
  return(urls)

}


#' get table data for a single url request
#' @param url string of a single url
#' @param p used as iterator in multicore processing
get_table_data <- function(url, p){
  p()
  
  httr::GET(url) %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$value} %>%
    as_tibble() %>%
    mutate_all(., as.character)
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
                           verbose=TRUE){
  

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
    
  #run get_polis_table iteratively over all tables
  defaults <- load_defaults() %>%
    filter(grepl("RefData", table_name)) #Note: This filter is in place for development purposes - to reduce the time needed for testing. Remove for final
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
                    download_size = download_size)
  }
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
