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
init_polis_data_table <- function(table_name, field_name){
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
        "latest_date"=as_date("1900-01-01"),
        "date_field" = field_name
      )) %>%
      write_rds(cache_file)
    #Create empty destination rds
    my_url4 <-  paste0('https://extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages&$top=1",
                       '&token=',load_specs()$polis$token) 
    result4 <- httr::GET(my_url4)
    result_content4 <- httr::content(result4, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    empty_table <- result_content4$value %>% head(0)
    write_rds(empty_table, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
  }
}

#fx2: read cache_dir and return table-name, last-update, and latest-date to the working environment
read_table_in_cache_dir <- function(table_name){
  folder <- load_specs()$polis_data_folder
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
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
date_min_conv <- function(field_name, date_min){
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



create_api_url <- function(table_name, latest_date, field_name){
  table_name <<- table_name
  latest_date <<- latest_date
  field_name <<- field_name
  min_date <- latest_date
  max_date <- NULL
  filter_url_conv <- make_url_general(
    field_name,
    min_date,
    max_date
  )
  token <- load_specs()$polis$token
  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                  paste0(table_name, "?"),
                  "$filter=",
                  if(filter_url_conv == "") "" else paste0(filter_url_conv),
                  '&token=',token) %>%
    httr::modify_url()
  
  return(my_url)
}

#fx4: Query POLIS via the API url created in fx3

polis_data_pull <- function(my_url, verbose=TRUE){
  token <- load_specs()$polis$token
  all_results <- NULL
  initial_query <- my_url
  i <- 1
  
  #loop through the while loop, with each iteration pulling 2000 rows via API, then getting the next URL
  while(!is.null(my_url)){
    cycle_start <- Sys.time()
    result <- httr::GET(my_url) 
    result_content <- httr::content(result,type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    all_results <- bind_rows(all_results,mutate_all(result_content$value,as.character))
    table_count <- result_content$odata.count
    my_url <- result_content$odata.nextLink #Get the next URL (e.g. URL + skip=2000) and return to start of while loop
    #Get total queries on initial pass-through
    if(i == 1){
      total_queries <- ceiling(as.numeric(table_count)/nrow(result_content$value))
      } 
    cycle_end <- Sys.time()
    cycle_time <- round(as.numeric(difftime(cycle_end, cycle_start, units="secs")), 1)
    if(verbose == TRUE) print(paste0('Completed query ', i, " of ", total_queries, "; Query time: ", cycle_time, " seconds"))
    i <- i + 1
  }
  
  #Get full table size for comparison to what was pulled via API, saved as "table_count2"
    my_url2 <-  paste0('https://extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages&$top=0",
                       '&token=',load_specs()$polis$token) %>%
                httr::modify_url()
    result2 <- httr::GET(my_url2)
    result_content2 <- httr::content(result2, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    table_count2 <- result_content2$odata.count

  #If full table size is greater than what's been pulled, then pull in missing field_name rows
  if(as.numeric(table_count2) > as.numeric(nrow(all_results))){
    my_url3 <- paste0('https://extranet.who.int/polis/api/v2/',
                      paste0(table_name, "?"),
                      "$filter=(",
                      field_name,
                      " eq null)&$inlinecount=allpages",
                      '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
    while(!is.null(my_url3)){
      result_na <- httr::GET(my_url3)
      result_na_content <- httr::content(result_na,type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
      if(!is.null(nrow(result_na_content$value))) {
      all_results <- bind_rows(all_results,mutate_all(result_na_content$value,as.character))
      my_url3 <- result_na_content$odata.nextLink
      }
      if(is.null(nrow(result_na_content$value))) {
        my_url3 <- NULL
      }
    }
  }
  attr(all_results,'query') = initial_query
  return(all_results)
}

append_and_save <- function(query_output = query_output,
                            id_vars = id_vars, #id_vars is a vector of data element names that, combined, uniquely identifies a row in the table
                            table_name = table_name){
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
  if(table_count2 == nrow(old_polis) + nrow(query_output)){
    query_output <- query_output %>%
      bind_rows(old_polis)
    write_rds(query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
  }
  if(table_count2 != nrow(old_polis) + nrow(query_output)){
      stop("Table is incomplete: check id_vars and field_name")
  }
}

# fx5: calculates new last-update and latest-date and enters it into the cache, saves the dataset as rds
get_update_cache_dates <- function(query_output, field_name, table_name){
    temp <- query_output %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d"))
    latest_date <<- as.Date(max(temp[,1]), "%Y-%m-%d")
    updated <<- Sys.time()
}
    
#Feature: Validate POLIS Pull (#8): Create POLIS validation metadata and store in cache
get_polis_metadata <- function(query_output,
                               table_name,
                               field_name
                               ){

  #summarise var names and classes
  var_name_class <- skimr::skim(query_output) %>% 
    select(skim_type, skim_variable, character.n_unique) %>%
    rename(var_name = skim_variable,
           var_class = skim_type)
  
  #categorical sets
  categorical_vars <- query_output %>%
    select(var_name_class$var_name[var_name_class$character.n_unique <= 30]) %>%
    pivot_longer(cols=everything(), names_to="var_name", values_to = "response") %>%
    distinct() %>%
    pivot_wider(names_from=var_name, values_from=response, values_fn = list) %>%
    pivot_longer(cols=everything(), names_to="var_name", values_to="categorical_response_set")
  
  table_metadata <- var_name_class %>%
    select(-character.n_unique) %>%
    left_join(categorical_vars, by=c("var_name"))
  
  write_rds(table_metadata, file.path(load_specs()$polis_data_folder, "cache_dir", paste0(table_name, "_metadata.rds")))  
    }

#read metadata from cache and compare to new file
read_table_metadata <- function(table_name){
  table_metadata <- read_rds( file.path(load_specs()$polis_data_folder, "cache_dir", paste0(table_name, "_metadata.rds")))
}
#Compare metadata of newly pulled dataset to cached metadata
compare_metadata <- function(query_output,
                             table_metadata){
  
  #get new metadata
    #summarise var names and classes
  query_output <- query_output %>%
      filter(SiteStatus != "INACTIVE") %>%
      mutate(SiteStatus = case_when(SiteStatus == "ACTIVE" ~ "TESTING",
                                    TRUE ~ SiteStatus)) 
    var_name_class <- skimr::skim(query_output) %>% 
      select(skim_type, skim_variable, character.n_unique) %>%
      rename(new_var_name = skim_variable,
             new_var_class = skim_type)
    
    #categorical sets
    categorical_vars <- query_output %>%
      select(var_name_class$new_var_name[var_name_class$character.n_unique <= 30]) %>%
      pivot_longer(cols=everything(), names_to="new_var_name", values_to = "new_response") %>%
      distinct() %>%
      pivot_wider(names_from=new_var_name, values_from=new_response, values_fn = list) %>%
      pivot_longer(cols=everything(), names_to="new_var_name", values_to="new_categorical_response_set")
  
    new_metadata <- var_name_class %>%
      select(-character.n_unique) %>%
      left_join(categorical_vars, by=c("new_var_name"))
    
    #compare to old metadata
    compare_metadata <- table_metadata %>%
      full_join(new_metadata, by=c("var_name" = "new_var_name"))
    
    new_vars <- (compare_metadata %>%
      filter(is.na(var_class)))$var_name
    if(length(new_vars) != 0){
      new_vars <<- new_vars
      warning(print("There are new variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'new_vars'"))
    }
      
    lost_vars <- (compare_metadata %>%
      filter(is.na(new_var_class)))$var_name
    
    if(length(lost_vars) != 0){
      lost_vars <<- lost_vars
      warning(print("There are missing variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'lost_vars'"))
    }
    
    class_changed_vars <- compare_metadata %>%
      filter(var_class != new_var_class &
               !(var_class %in% new_vars) &
               !(var_class %in% lost_vars)) %>%
      select(-c(categorical_response_set, new_categorical_response_set)) %>%
      rename(old_var_class = var_class)
    
    if(nrow(class_changed_vars) != 0){
      class_changed_vars <<- class_changed_vars
      warning(print("There are variables in the POLIS table with different classes\ncompared to when it was last retrieved\nReview in 'class_changed_vars'"))
    }
    
    new_response <- compare_metadata %>%
      filter(!(var_name %in% class_changed_vars$old_var_class) &
             !(var_class %in% new_vars) &
             !(var_class %in% lost_vars)) %>%
      rowwise() %>%
      mutate(same = toString(intersect(categorical_response_set, new_categorical_response_set)),
             in_old_not_new = toString(setdiff(categorical_response_set, new_categorical_response_set)),
             in_new_not_old = toString(setdiff(new_categorical_response_set, categorical_response_set))) %>%
      filter(in_new_not_old != "") %>%
      rename(old_categorical_response_set = categorical_response_set) %>%
      select(var_name, old_categorical_response_set, new_categorical_response_set, same, in_old_not_new, in_new_not_old)
    
    if(nrow(new_response) != 0){
      new_response <<- new_response
      warning(print("There are categorical responses in the new table\nthat were not seen when it was last retrieved\nReview in 'new_response'"))
    }
    
    #Create an inidicator that is TRUE if there has been a change in table structure or content that requires re-pulling of the table
    re_pull_polis_indicator <<- FALSE
    if(nrow(new_response) != 0 |
       nrow(class_changed_vars) != 0 |
       length(lost_vars) != 0 |
       length(new_vars) != 0){
      re_pull_polis_indicator <<- TRUE
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
get_polis_table <- function(folder = Sys.getenv("polis_data_folder"),#or use load_specs() 
                            token = load_specs()$polis$token, 
                            table_name,
                            field_name,
                            verbose = TRUE,
                            id_vars = "Id"){
  init_polis_data_struc(folder, token)
  
  init_polis_data_table(table_name, field_name)
  
  x <- read_table_in_cache_dir(table_name)
  
  query_output <- polis_data_pull(my_url = create_api_url(table_name, x$latest_date, x$field_name), 
                                  verbose = TRUE)
  
  append_and_save(query_output = query_output,
                  table_name = table_name,
                  id_vars = id_vars)
  
  get_update_cache_dates(query_output = query_output,
                         field_name = field_name,
                         table_name = table_name)
  
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

# Examples of get_polis_table:
get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Lqas",
                field_name = "Start",
                id_vars = "Id",
                verbose=TRUE)


get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Synonym",
                field_name = "CreatedDate",
                id_vars = "Id",
                verbose=TRUE)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "EnvSample",
                field_name = "LastUpdateDate",
                id_vars = "Id",
                verbose=TRUE)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "UpdatedDate",
                id_vars = "Id",
                verbose=TRUE)
