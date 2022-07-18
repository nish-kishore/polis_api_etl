#' get table data for a single url request
#' @param url string of a single url
#' @param p used as iterator in multicore processing
get_table_data <- function(url, p){tryCatch({
  status_code <- "x"
  i <- 1
  while(status_code != "200" & i < 10){
    p()
    result <- NULL
    result <- httr::GET(url, timeout(150))
    if(is.null(result) == FALSE){
      status_code <- as.character(result$status_code)
    }
    i <- i+1
    if(i == 10){
      # if(file.exists(paste0(load_specs()$polis_data_folder, "/", table_name,"_failed_urls.rds"))){
      #   all_failed_urls <- readRDS(paste0(load_specs()$polis_data_folder, "/", table_name,"_failed_urls.rds"))
      #   all_failed_urls <- c(all_failed_urls, url)
      #   }
      # if(file.exists(paste0(load_specs()$polis_data_folder, "/", table_name,"_failed_urls.rds")) == FALSE){
      #   all_failed_urls <- url
      # }
      # write_rds(all_failed_urls, paste0(load_specs()$polis_data_folder, "/", table_name,"_failed_urls.rds"))
      result <- NULL
    }
    if(status_code != "200"){
      Sys.sleep(10)
    }
  }
  rm(status_code)
  
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