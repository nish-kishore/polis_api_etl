#' get table data for a single url request
#' @param url string of a single url
#' @param p used as iterator in multicore processing
get_table_data <- function(url, p){tryCatch({
  
  p()
  result <- call_url(url=url,
                      error_action = "RETURN NULL")
  
  if(is.null(result) == FALSE){
    response_data <- result %>%
      httr::content(type='text',encoding = 'UTF-8') %>%
      jsonlite::fromJSON() %>%
      {.$value} %>%
      as_tibble() %>%
      mutate_all(., as.character)
  }
  if(is.null(result) == TRUE){
    response_data <- NULL
  }
  response_data <- tibble(url = c(url), data = list(response_data))
  return(response_data)
}, error=function(e){
  response_data <- list(url = c(url), data = NULL)
  return(response_data)
})
}

#' multicore pull from API
#' @param urls array of URL strings
mc_api_pull <- function(urls){
  p <- progressor(steps = length(urls))
  future_map(urls,get_table_data, p = p) %>%
    rbind()
}

#' wrapper around multicore pull to produce progress bars
#' @param urls array of URL strings
pb_mc_api_pull <- function(urls){
  n_cores <- availableCores() - 1
  plan(multicore, workers = n_cores, gc = T)
  
  with_progress({
    result <- mc_api_pull(urls)
  })
  
  failed_urls <- c()
  
  #extract data from result and combine into result_df
  for(i in 1:length(result)){
    data <- result[[i]]$data[[1]]
    if(i == 1){
      result_df <- data
    }
    if(i != 1){
      result_df <- result_df %>%
        bind_rows(data)
    }
    #If the url failed, extract the url from result and add to the failed_urls list
    if(is.null(data)){
      failed_urls <- c(failed_urls, result[[i]]$url[1])
    }
  }
  #Combine the full dataset and failed_urls list and return
  result <- list(result_df, failed_urls)
  return(result)
  stopCluster(n_cores)
}

#retry all urls that failed when calling a url array
handle_failed_urls <- function(failed_urls,
                               failed_url_filename,
                               query_output, 
                               retry = TRUE, 
                               save = TRUE){
  if(length(failed_urls) > 0){
    if(retry == TRUE){
      retry_query_output_list <- pb_mc_api_pull(failed_urls)
      retry_query_output <- retry_query_output_list[[1]]
      if(is.null(retry_query_output)){
        retry_query_output <- data.frame(matrix(nrow=0, ncol=0))
      }
      failed_urls <- retry_query_output_list[[2]]
      query_output <- query_output %>% 
        bind_rows(retry_query_output)
    }
    if(save == TRUE){
      write_rds(failed_urls, failed_url_filename)
    }
  }
  return(query_output)
}

#Join the previously cached dataset for a table to the newly pulled dataset
append_and_save <- function(query_output = query_output,
                            id_vars = load_query_parameters()$id_vars, #id_vars is a vector of data element names that, combined, uniquely identifies a row in the table
                            table_name = load_query_parameters()$table_name,
                            full_idvars_output = full_idvars_output){
  
  id_vars <- as.vector(id_vars)
  
  #remove records that are no longer in the POLIS table from query_output
  if(nrow(full_idvars_output) > 0){
    query_output <- find_and_remove_deleted_obs(full_idvars_output = full_idvars_output,
                                                new_complete_file = query_output,
                                                id_vars = id_vars)
  }
  #If the newly pulled dataset has any data, then read in the old file, remove rows from the old file that are in the new file, then bind the new file and old file
  if(!is.null(query_output) & nrow(query_output) > 0 & file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
    old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
      mutate_all(.,as.character) %>%
      #remove records that are in new file
      anti_join(query_output, by=id_vars) 
    #remove records that are no longer in the POLIS table from old_polis
    if(nrow(full_idvars_output) > 0){
      old_polis <- find_and_remove_deleted_obs(full_idvars_output = full_idvars_output,
                                               new_complete_file = old_polis,
                                               id_vars = id_vars)
    }
    
    
    new_query_output <- query_output %>%
      bind_rows(old_polis)
    #save to file
    write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
    return(new_query_output)
    
    #If the overall number of rows in the table is not equal to old and new combined, then stop and flag for investigation
    #NOTE: instead of flagging, this could just trigger a re-pull of the full dataset
    if(table_count2 != nrow(old_polis) + nrow(query_output)){
      warning("Table is incomplete: check id_vars and field_name")
    }
  }
  if(!is.null(query_output) & nrow(query_output) > 0 & !file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
    #check that the combined total row number matches POLIS table row number before appending
    #Get full table size for comparison to what was pulled via API, saved as "table_count2"
    my_url2 <-  paste0('https:/extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages&$top=0",
                       '&token=',load_specs()$polis$token) %>%
      httr::modify_url()
    
    #The below while() loop runs my_url2 through the API until it succeeds or up to 10 times. If 10 try limit is reached, then the process is halted.
    result2 <- call_url(url=my_url2,
                        error_action = "STOP")
    
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

#function to remove the obs deleted from the POLIS table from the saved table
find_and_remove_deleted_obs <- function(full_idvars_output,
                                        new_complete_file,
                                        id_vars){
  id_vars <- as.vector(id_vars)
  deleted_obs <- new_complete_file %>%
    select(id_vars) %>%
    anti_join(full_idvars_output, by=id_vars)
  new_complete_file <- new_complete_file %>%
    anti_join(deleted_obs, by=id_vars)
  return(new_complete_file)
}

#combine calling urls and processing output into a single function
call_urls_combined <- function(urls,
                               type){
  if(type == "full"){print("Pulling all variables:")}
  if(type == "id_filter"){print("Pulling ID variables:")}
  if(type == "id_only"){print("Checking for deleted Ids in the full table:")}
  query_start_time <- Sys.time()
  query_output <- data.frame(matrix(nrow=0, ncol=0))
  query_output_list <- pb_mc_api_pull(urls)
  query_output <- query_output_list[[1]]
  if(is.null(query_output)){
    query_output <- data.frame(matrix(nrow=0, ncol=0))
  }
  failed_urls <- query_output_list[[2]]
  if(type == "full"){
    failed_url_filename <- file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name,"_failed_urls.rds"))
  }
  if(type == "id_filter"){
    failed_url_filename <- file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name,"_id_filter_failed_urls.rds"))
  }
  if(type == "id_only"){
    failed_url_filename <- file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name,"_id_only_failed_urls.rds"))
  }
  query_output <- handle_failed_urls(failed_urls,
                                     failed_url_filename,
                                     query_output,
                                     retry = TRUE,
                                     save = TRUE)
  query_stop_time <- Sys.time()
  query_time <- round(difftime(query_stop_time, query_start_time, units="auto"),0)
  if(type == "full" & !is.null(query_output)){
    print(paste0("Downloaded ", nrow(query_output)," rows from ",load_query_parameters()$table_name_descriptive," Table in ", query_time[[1]], " ", units(query_time),"."))
  }
  return(query_output)
}