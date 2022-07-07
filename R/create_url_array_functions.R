create_url_array_id_method <- function(table_name,
                                       id_vars,
                                       field_name,
                                       min_date = min_date){
  prior_scipen <- getOption("scipen")
  options(scipen = 999)
  
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
  options(scipen=prior_scipen)
  return(urls)
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
  
  response <- httr::GET(my_url2, timeout(150))
  
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
  
  prior_scipen <- getOption("scipen")
  options(scipen = 999)
  urls <- paste0(my_url, "&$top=", as.numeric(download_size), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(download_size)))
  options(scipen=prior_scipen)
  
  return(urls)
  
}


