#Utility functions for the POLIS API ETL

#' Check to see if cache exists, if not create it
#' @param folder A string, the location of the polis data folder
#' @return A string describing the creation process or errors
init_polis_data_struc <- function(folder, token = NULL){
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
  print("Cache updated!")
  return(tmp[which(tmp$file_name == .file_name),])
}


#fx1: check to see if table exists in cache_dir. If not, last-updated and last-item-date are default min and entry is created; if table exists no fx necessary
init_polis_data_table <- function(table_name, field_name){
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

    #Create empty destination rds
      #Get colnames for empty destination rds
        my_url4 <-  paste0('https://extranet.who.int/polis/api/v2/',
                           paste0(table_name, "?"),
                           "$inlinecount=allpages&$top=1",
                           '&token=',load_specs()$polis$token)
        result4 <- httr::GET(my_url4)
        result_content4 <- httr::content(result4, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
      #save colnames and 0 rows as empty dataframe
      empty_table <- result_content4$value %>% head(0)

      write_rds(empty_table, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
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

#convert field_name and date_max to the format needed for API query

#' @param field_name The name of the field used for date filtering.
#' @param date_max The 10 digit string for the max date.
#' @return String compatible with API v2 syntax.
date_max_conv <- function(field_name, date_max){
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
make_url_general <- function(field_name,
                            min_date,
                            max_date,
                            ...){

  paste0(c(date_min_conv(field_name, min_date),
           date_max_conv(field_name, max_date)),
         collapse = " and ") %>%
    paste0(...) %>%
    paste0("&$inlinecount=allpages")
}

#Join the previously cached dataset for a table to the newly pulled dataset
append_and_save <- function(query_output = query_output,
                            id_vars = id_vars, #id_vars is a vector of data element names that, combined, uniquely identifies a row in the table
                            table_name = table_name){

  #If the newly pulled dataset has any data, then read in the old file, remove rows from the old file that are in the new file, then bind the new file and old file
  if(!is.null(query_output)){
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
    write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
    return(new_query_output)
    }

    #If the overall number of rows in the table is not equal to old and new combined, then stop and flag for investigation
      #NOTE: instead of flagging, this could just trigger a re-pull of the full dataset
      if(table_count2 != nrow(old_polis) + nrow(query_output)){
      stop("Table is incomplete: check id_vars and field_name")
  }
}
}

# fx5: calculates new last-update and latest-date and enters it into the cache, saves the dataset as rds
get_update_cache_dates <- function(query_output,
                                   field_name,
                                   table_name){

    #If the newly pulled dataset contains any data, then pull the latest_date as the max of field_name in it
    if(!is.null(query_output)){
    temp <- query_output %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d")) %>%
      as.data.frame()
    latest_date <<- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
    }

    #If the newly pulled dataset is empty (i.e. there is no new data since the last pull), then pull the latest_date as teh max of field_name in the old dataset
    if(is.null(query_output)){
    old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
      mutate_all(.,as.character)
    temp <- old_polis %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d")) %>%
      as.data.frame()
    latest_date <<- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
  }
      #Save the current system time as the date/time of 'updated' - the date the database was last checked for updates
      updated <<- Sys.time()
}

#Feature: Validate POLIS Pull (#8): Create POLIS validation metadata and store in cache
get_polis_metadata <- function(query_output,
                               table_name,
                               categorical_max = 30){
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

      #overwrite table rds with empty destination rds
      write_rds(data.frame(matrix(ncol=0, nrow=0)), file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
  }
  #re-pull complete table
  x <- read_table_in_cache_dir(table_name)
  query_output <- polis_data_pull(my_url = create_api_url(table_name, as.Date(x$updated, "%Y-%m-%d"), x$field_name),
                                  verbose = TRUE)
  }
}


#Input function using fx1:fx5

#' @param folder      A string, the location of the polis data folder
#' @param token       A string, the token for the API
#' @param table_name  A string, matching the POLIS name of the requested data table
#' @param field_name  A string, the name of the variable in the requested data table used to filter API query
#' @param verbose     A logic value (T/F), used to indicate if progress notes should be printed while API query is running
#' @param id_vars     A vector of variables that, in combination, uniquely identify rows in the requested table
#'
get_polis_table <- function(folder = load_specs()$polis_data_folder,
                            token = load_specs()$polis$token,
                            table_name,
                            field_name,
                            verbose = TRUE,
                            id_vars = "Id"){
  #Create POLIS data folder structure if it does not already exist
  init_polis_data_struc(folder, token)

  #Create cache entry and blank dataframe for a POLIS data table if it does not already exist
  init_polis_data_table(table_name, field_name)

  #Read the cache entry for the requested POLIS data table
  x <- read_table_in_cache_dir(table_name)

  #Create an API URL and use it to query POLIS
  urls <- create_url_array(table_name = table_name,
                           field_name = field_name)
  
  query_output <- pb_mc_api_pull(urls)  
  
  #If the query produced any output, summarise it's metadata
  new_table_metadata <- NULL
  if(!is.null(query_output)){
  new_table_metadata <- get_polis_metadata(query_output = query_output,
                                           table_name = table_name)
  }

  #If a version of the data table has been previously pulled and saved, read it in and summarize it's metadata for comparison
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

  #If a re-pull was indicated in the metadata comparison, then re-pull the full table
  query_output_repull <- NULL
  if(re_pull_polis_indicator == TRUE){
    #Read the cache entry for the requested POLIS data table
    x <- read_table_in_cache_dir(table_name)
    
    #Create an API URL and use it to query POLIS
    urls <- create_url_array(table_name = table_name,
                             field_name = field_name)
    
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
                           min_date = as_date("2000-01-01"),
                           field_name){

  filter_url_conv <- make_url_general(
    field_name,
    min_date,
    max_date
  )

  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=",
                   if(filter_url_conv == "") "" else paste0(filter_url_conv),
                   '&token=',load_specs()$polis$token,
                   "&$top=0") %>%
    httr::modify_url()

  response <- httr::GET(my_url)

  response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer() %>%
    return()
}

#' @param
get_table_count_missing <- function(table_name,
                            field_name){
  
  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=(",
                   field_name,
                   " eq null)&$inlinecount=allpages",
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  
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
                            max_date = NULL,
                            field_name = x$field_name,
                            download_size = 500){
  #first, create a set of urls with the date filter
  filter_url_conv <- make_url_general(
    field_name,
    min_date,
    max_date
  )

  my_url1 <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=",
                   if(filter_url_conv == "") "" else paste0(filter_url_conv),
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()

  table_size <- get_table_count(table_name = table_name,
                                min_date = min_date,
                                field_name = field_name)

  urls1 <- paste0(my_url1, "&$top=", download_size, "&$skip=",seq(0,table_size, by = download_size))
  
  #second create a set of urls filtered to where the date field is missing
  my_url2 <- paste0('https://extranet.who.int/polis/api/v2/',
                    paste0(table_name, "?"),
                           "$filter=(",
                           field_name,
                           " eq null)&$inlinecount=allpages",
                           '&token=',load_specs()$polis$token) %>%
                      httr::modify_url()
  
  table_size_missing <- get_table_count_missing(table_name = table_name,
                                                field_name = field_name)
  if(table_size_missing != 0){
  urls2 <- paste0(my_url2, "&$top=", download_size, "&$skip=",seq(0,table_size_missing, by = download_size))
  urls <- c(urls1, urls2)
  }
  if(table_size_missing == 0){
  urls <- urls1
  }
  
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


