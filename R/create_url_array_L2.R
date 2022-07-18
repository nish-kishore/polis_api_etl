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

#' create a URL to collect the count where field_name is not missing
#' @param
get_table_count <- function(table_name,
                            min_date = as_date("1900-01-01"),
                            field_name){
  
  filter_url_conv <- make_url_general(
    field_name,
    min_date
  )
  #If no date field, then get the full table size:
  if(field_name == "None"){
    my_url <- paste0('https:/extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$inlinecount=allpages",
                     '&token=',load_specs()$polis$token,
                     "&$top=0") %>%
      httr::modify_url()
  }
  #If there is a date field, then filter the full table to the requested date range and get the resulting table size
  if(field_name != "None"){
    my_url <- paste0('https:/extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$filter=",
                     if(filter_url_conv == "") "" else paste0(filter_url_conv),
                     '&token=',load_specs()$polis$token,
                     "&$top=0") %>%
      httr::modify_url()
  }
  
  #Call my_url until the call succeeds or is tried 10 times:
  response <- call_url(url = my_url,
           error_action = "STOP")
  
  response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer() %>%
    return()
}



#' create an array of URL's for a given table using the skip/top method
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
  #If there is no date filter, create a url without a date filter:
  if(field_name == "None"){
    my_url <- paste0('https:/extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$inlinecount=allpages",
                     '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
  }
  #If there is a data filter, create a url with the date filter:
  if(field_name != "None"){
    my_url <- paste0('https:/extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$filter=",
                     if(filter_url_conv == "") "" else paste0(filter_url_conv),
                     '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
  }
  #Get the full table size, used for creating the url array:
  table_size <- get_table_count(table_name = table_name,
                                min_date = min_date,
                                field_name = field_name)
  
  prior_scipen <- getOption("scipen")
  options(scipen = 999)
  urls <- paste0(my_url, "&$top=", as.numeric(download_size), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(download_size)))
  options(scipen=prior_scipen)
  
  return(urls)
  
}

#Create url array using ID filter method
create_url_array_idvars_and_field_name <- function(table_name = table_name,
                                                   id_vars = id_vars,
                                                   field_name = field_name,
                                                   min_date = min_date){
  # construct general URL
  filter_url_conv <- paste0("(",
                            date_min_conv(field_name, min_date),
                            ") or (",
                            date_null_conv(field_name), ")")
  
  my_url <- paste0('https:/extranet.who.int/polis/api/v2/',
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
  
  response <- call_url(url=my_url,
                       error_action = "STOP")
  
  table_size <- response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer()
  # build URL array
  prior_scipen <- getOption("scipen")
  options(scipen = 999)
  urls <- paste0(my_url, "&$top=", as.numeric(1000), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(1000)))
  options(scipen=prior_scipen)
  return(urls)
}



create_url_array_id_section <- function(table_name = table_name,
                                        id_section_table = id_section_table){
  urls <- c()
  for(i in id_section_table$filter_url_conv){
    url <- paste0('https:/extranet.who.int/polis/api/v2/',
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
  prior_scipen <- getOption("scipen")
  options(scipen = 999)
  
  get_ids_for_url_array(table_name, id_vars, field_name, min_date)
  id_list <- readRDS(file.path(load_specs()$polis_data_folder,"id_list_temporary_file.rds"))
  id_list2 <- id_list %>% 
    select(id_vars) %>%
    unique() %>%
    mutate_all(as.numeric)
  colnames(id_list2) <- c("Id")
  id_list2 <- id_list2 %>%
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
        mutate(filter_url_conv = paste0("((",id_vars," ge ", start, ") and (",id_vars," le ", end,"))"))
    }
    if(i != 1){
      id_section_table <- id_section_table %>%
        bind_rows(data.frame(start=break_list_start, end=break_list_end, min=min_id, max=max_id) %>%
                    mutate_all(as.numeric) %>%
                    rowwise() %>%
                    mutate(end = ifelse(end > max, max, end)) %>%
                    ungroup() %>%
                    mutate(filter_url_conv = paste0("((",id_vars," ge ", start, ") and (",id_vars," le ", end,"))")))
    }
  }  
  #create list of seq first to last by 1000
  urls <- create_url_array_id_section(table_name, id_section_table)
  options(scipen=prior_scipen)
  return(urls)
}



#Check if the specified id variable exists in the table:
check_if_id_exists <- function(table_name,
                               id_vars = "Id"){
  url <-  paste0('https:/extranet.who.int/polis/api/v2/',
                 paste0(table_name, "?"),
                 '$select=', 
                 paste(id_vars, collapse=", "),
                 '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  
  response <- call_url(url=url,
                       error_action = "STOP")
  
  status <- as.character(response$status_code)
  id_exists <- ifelse(status=="200", TRUE, FALSE)
  return(id_exists)
}

#Get all ids for id-filter method
get_ids_for_url_array <- function(table_name,
                                  id_vars,
                                  field_name,
                                  min_date = min_date){
  urls <- create_url_array_idvars_and_field_name(table_name, id_vars, field_name, min_date)
  query_output_list <- pb_mc_api_pull(urls)
  id_list <- query_output_list[[1]]
  id_list_failed_urls <- query_output_list[[2]]
  id_list <- handle_failed_urls(id_list_failed_urls,
                                file.path(load_specs()$polis_data_folder, paste0(table_name,"_id_list_failed_urls.rds")),
                                id_list,
                                retry = TRUE,
                                save = TRUE)
  write_rds(id_list, file.path(load_specs()$polis_data_folder,"id_list_temporary_file.rds"))
}


#for any table, pull just the idvars for a check for deletions in POLIS and a
# a double-check for any additions in POLIS not captured by the date_field query

create_url_array_idvars <- function(table_name = table_name,
                                    id_vars = id_vars){
  # construct general URL
  my_url <- paste0('https:/extranet.who.int/polis/api/v2/',
                   paste0(table_name, "?"),
                   "$select=", paste(id_vars, collapse=","),
                   "&$inlinecount=allpages",
                   '&token=',load_specs()$polis$token) %>%
    httr::modify_url()
  # Get table size
  my_url2 <- paste0('https:/extranet.who.int/polis/api/v2/',
                    paste0(table_name, "?"),
                    "$inlinecount=allpages",
                    '&token=',load_specs()$polis$token,
                    "&$top=0") %>%
    httr::modify_url()
  
  response <- call_url(url=my_url2,
                     error_action = "STOP")
  
  table_size <- response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer()
  # build URL array
  urls <- paste0(my_url, "&$top=", as.numeric(1000), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(1000)))
  return(urls)
}