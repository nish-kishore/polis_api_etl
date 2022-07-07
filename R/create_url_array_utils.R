# create_url_array_id_section
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

# get_ids_for_url_array
get_ids_for_url_array <- function(table_name,
                                  id_vars,
                                  field_name,
                                  min_date = min_date){
  urls <- create_url_array_idvars_and_field_name(table_name, id_vars, field_name, min_date)
  id_list <- pb_mc_api_pull(urls)
  write_rds(id_list, paste0(load_specs()$polis_data_folder,"/id_list_temporary_file.rds"))
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
  
  status_code <- "x"
  i <- 1
  while(status_code != "200" & i < 10){
    response <- NULL
    response <- httr::GET(my_url, timeout(150))
    if(is.null(response) == FALSE){
      status_code <- as.character(response$status_code)
    }
    i <- i+1
    if(i == 10){
      stop("Query halted. Repeated API call failure.")
    }
    Sys.sleep(10)
  }
  rm(status_code)
  
  response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer() %>%
    return()
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
