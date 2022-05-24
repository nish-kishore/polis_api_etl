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
    write_rds(data.frame(matrix(ncol = 0, nrow = 0)), file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
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



create_api_url <- function(table_name, 
                           latest_date = as_date("2000-01-01"), 
                           field_name){
  table_name <- table_name
  field_name <- field_name
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
  
  #Check if field_name selected has missing values that will be excluded from query, and notify user

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
    cycle_time <- round(as.numeric(difftime(cycle_end, cycle_start, units="secs")), 1)
    if(latest_date == "1900-01-01" & i == 1){
      my_url2 <-  paste0('https://extranet.who.int/polis/api/v2/',
                         paste0(table_name, "?"),
                        "$inlinecount=allpages&$top=0",
                        '&token=',token) 
      result2 <- httr::GET(my_url2)
      result_content2 <- httr::content(result2, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
      table_count2 <- result_content2$odata.count
      if(as.numeric(table_count2) > as.numeric(table_count)){
        warning(print(paste0("The selected field date ('", field_name, "') includes ", as.numeric(table_count2) - as.numeric(table_count), " obs with missing values. These will be excluded from the dataset.")))
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
  write_rds(all_results, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
  return(all_results)
}

# fx5: calculates new last-update and latest-date and enters it into the cache, saves the dataset as rds
get_update_cache_dates <- function(all_results, field_name, table_name){
    temp <- all_results %>%
      select(field_name) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d"))
    latest_date <<- as.Date(max(temp[,1]), "%Y-%m-%d")
    updated <<- Sys.time()
}
    

#Input function using fx1:fx5

#' @param folder      A string, the location of the polis data folder 
#' @param token       A string, the token for the API
#' @param table_name  A string, matching the POLIS name of the requested data table
#' @param field_name  A string, the name of the variable in the requested data table used to filter API query
#' @param verbose     A logic value (T/F), used to indicate if progress notes should be printed while API query is running
#' 
get_polis_table <- function(folder = Sys.getenv("polis_data_folder"),#or use load_specs() 
                            token = load_specs()$polis$token, 
                            table_name,
                            field_name,
                            verbose){
  init_polis_data_struc(folder, token)
  
  init_polis_data_table(table_name, field_name)
  
  x <- read_table_in_cache_dir(table_name)
  
  query_output <- polis_data_pull(my_url = create_api_url(table_name, x$latest_date, x$field_name), 
                                  verbose)
  
  get_update_cache_dates(all_results = query_output,
                         field_name)
  
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

# #fx4 with multicore: Query POLIS via the API url created in fx3 using parallel queries and multicore processing
# polis_data_pull_single_cycle <- function(my_url_cycle){
#   result <- httr::GET(my_url_cycle)
#   result_content <- httr::content(result,type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
#   cycle_results <- mutate_all(result_content$value,as.character)
#   return(cycle_results)
# }

#Polis_data_pull_multicore is meant to replace "polis_data_pull" function, once it is working
polis_data_pull_multicore <- function(my_url, 
                                      cycle_size = NULL){
  #If cycle size is not specified, pull the default from the API
  if(is.null(cycle_size)){
    cycle_size <- nrow((httr::content(httr::GET(my_url),type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON())$value)
  }
  
  #get the overall table size, used to create the cycle list
  table_size <- as.numeric((httr::content(httr::GET(my_url),type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON())$odata.count)
  
  #create a list of urls with skip/top pattern
  cycle_list <- paste0(my_url,"&$top=", cycle_size,"&$skip=", seq(0, table_size, by = cycle_size)) 
  
  all_results <- NULL
  initial_query <- my_url
  
  #batch the urls in cycle_list into a "list of lists" 
  batch_size <- 3
  number_of_sets <- ceiling(length(cycle_list) / batch_size)
  list_main <- NULL
  for(i in 1:number_of_sets){
    start <- (i*batch_size)-batch_size+1
    if((i*batch_size) > length(cycle_list)){
      end <- length(cycle_list)
    }
    else{end <- (i*batch_size)}
    list_main[[i]] =  as.list(cycle_list[start:end])
  }
  
  #For each batch of urls, run the queries in parallel for each item in the batch
  full_results <- NULL

  for(i in 1:length(list_main)) {
    if(i == 1){    total_starttime <- Sys.time()  }
    group_starttime <- Sys.time()

    #Create cluster
    closeAllConnections()
    no_cores <- parallel::detectCores()-1
    cl <- doParallel::registerDoParallel(no_cores)
    
    #pass batch i of URLs through cluster
    full_results[[i]] <- foreach(j=1:length(list_main[[i]]), #Currently, this works for i 1:7, regardless of the number of cores (i.e. if no_cores is set to 3, it can still run with i=1:5)
                           .combine=c, 
                           .packages = c("tidyverse", "httr", "jsonlite")) %dopar% {
                            (mutate_all((httr::content((httr::GET(as.character(list_main[[i]][j]))),type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON())$value,as.character))
                           }
    
    #Close cluster
    stopImplicitCluster()
    group_diff <- round(Sys.time() - group_starttime,0)
    print(paste0("group ",i, ", seconds: ", group_diff))
  }
  total_diff <- round(as.numeric(difftime(Sys.time(), total_starttime, units="secs")),0)
  print(paste0("total seconds: ", total_diff))
  
  
  return(full_results)
}  
  
#' create a URL to collect the count 
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

#' create an array of URL's for a given table 
#' @param table_name string of a table name available in POLIS
#' @param min_date 'date' object of earliest date acceptable for filter 
#' @param field_name string of field name used to filter date
#' @param download_size integer specifying # of rows to download
create_url_array <- function(table_name, 
                            min_date = as_date("2000-01-01"), 
                            max_date = NULL,
                            field_name,
                            download_size = 500){
  
  filter_url_conv <- make_url_general(
    field_name,
    min_date,
    max_date
  )
  
  my_url <- paste0('https://extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$filter=",
                   if(filter_url_conv == "") "" else paste0(filter_url_conv),
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  
  table_size <- get_table_count(table_name = table_name, 
                                min_date = min_date, 
                                field_name = field_name)
  
  urls <- paste0(my_url, "&$top=500&$skip=",seq(0,table_size,download_size))
  
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
    as_tibble()
}

#' multicole pull from API
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

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Synonym",
                field_name = "CreatedDate",
                verbose=TRUE)

