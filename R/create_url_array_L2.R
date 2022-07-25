#' date_min_conv
#' 
#' Creates the date filter snippet of a POLIS API URL call
#' 
#' @param field_name The name of the field used for date filtering.
#' @param date_min The 10 digit string for the min date.
#' @return String compatible with API v2 syntax.

#convert field_name and date_min to the format needed for API query
date_min_conv <- function(field_name = load_query_parameters()$field_name, 
                          date_min = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin)){
  if(is.null(field_name) || is.null(date_min)) return(NULL)
  paste0("(",
         paste0(field_name, " ge DateTime'",
                date_min, "'"),
         ")")
}

#' date_null_conv
#' 
#' Creates the date filter snippet of a POLIS API URL call corresponding to selecting rows where the date field is null
#' 
#' @param field_name The name of the field used for date filtering.
#' @param date_min The 10 digit string for the min date.
#' @return String compatible with API v2 syntax.
date_null_conv <- function(field_name = load_query_parameters()$field_name){
  if(is.null(field_name)) return(NULL)
  paste0("(",
         paste0(field_name, " eq null",
                ")"))
}

#' make_url_general
#' 
#' Combines the URL snippets from date_min_conv and date_null_conv into the full date filter portion of a POLIS API URL call
#'
#' @param field_name The date field to which to apply the date criteria, unique to each data type.
#' @param min_date Ten digit date string YYYY-MM-DD indicating the minimum date, default 2010-01-01
#' 
make_url_general <- function(field_name = load_query_parameters()$field_name,
                             min_date = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin)){
  
  paste0("(",
         date_min_conv(field_name, min_date),
         ") or (",
         date_null_conv(field_name), ")") 
}

#' Create URL Array
#' 
#' Create an array of URLs that combine to cover an entire POLIS table
#' 
#' @param table_name Name of POLIS table that the URLs will cover
#' @param min_date Date to start query from
#' @param field_name Name of date field in POLIS table to use to structure query
#' @param download_size Number of rows to pull in each URL call
#' @param id_vars A vector of variable names from the POLIS table that combine to create a unique ID
#' @param method API query method. Options: "skip_top", "id_filter", and "id_only".
#' @return A vector of URLs

create_url_array_combined <- function(table_name = load_query_parameters()$table_name,
                                      min_date = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin),
                                      field_name = load_query_parameters()$field_name,
                                      download_size = as.numeric(load_query_parameters()$download_size),
                                      id_vars = load_query_parameters()$id_vars,
                                      method = NULL){
  #Turn off scientific-notation for numbers
  prior_scipen <- getOption("scipen")
  options(scipen = 999)
  
  #Identify which method to use based on table_name, field_name, and id_vars  
  if(is.null(method)){
    if(load_query_parameters()$field_name == "None" |
       grepl("IndicatorValue", table_name) == TRUE){
      method <- "skip_top"
    }
    if(load_query_parameters()$field_name != "None" &
       grepl("IndicatorValue", table_name) == FALSE){
      method <- "id_filter"
    }
  }
  filter_url_conv <- make_url_general(field_name, min_date)
  if(method == "skip_top"){
    if(field_name == "None"){
      my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages",
                       '&token=',load_specs()$polis$token)
    }
    #If there is a data filter, create a url with the date filter:
    if(field_name != "None"){
      my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$filter=",
                       if(filter_url_conv == "") "" else paste0(filter_url_conv),
                       "&$inlinecount=allpages",
                       '&token=',load_specs()$polis$token) 
    }
  }
  if(method == "id_filter"){
    my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$filter=",
                     if(filter_url_conv == "") "" else paste0(filter_url_conv),
                     "&$select=", paste0(paste(id_vars, collapse=","), ", ", field_name),
                     "&$inlinecount=allpages",
                     '&token=',load_specs()$polis$token) 
    
  }
  if(method == "id_only"){
    my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                     paste0(table_name, "?"),
                     "$select=", paste(id_vars, collapse=","),
                     "&$inlinecount=allpages",
                     '&token=',load_specs()$polis$token) 
  }
  
  #Get table size
  my_url_table_size <- paste0(my_url_base, "&$top=0") %>%
    httr::modify_url()
  
  response <- call_url(url=my_url_table_size,
                       error_action = "STOP")
  
  table_size <- response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer()
  
  #modify url for html
  my_url <- my_url_base %>%
    httr::modify_url()
  
  #Create sequence for URLs
  urls <- paste0(my_url, "&$top=", as.numeric(download_size), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(download_size)))
  if(method == "id_filter" & table_size > 0){
    id_list <- call_urls_combined(urls = urls,
                                 type = "id_filter")
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
  }
  options(scipen = prior_scipen)
  return(urls)
}
                                      