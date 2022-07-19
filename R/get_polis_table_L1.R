#Extract and save a single POLIS table:
#If table was previously pulled, move previous version to archive folder
#Use cache entry to determine range of data pull
#Pull data from POLIS
#Compare pulled data metadata to previous pulled data metadata to determine if a full re-pull of the table is needed
#If re-pull is indicated, then re-pull the full table
#Update cache entry
#Save a summary of changes between new data pull and previous data pull

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
                            download_size = 1000,
                            table_name_descriptive = NULL,
                            check_for_deleted_rows = FALSE){
  
  #Create POLIS data folder structure if it does not already exist
  init_polis_data_struc(folder, token)
  
  get_query_parameters(table_name = table_name,
                       field_name = field_name, 
                       id_vars = id_vars,
                       download_size = download_size,
                       table_name_descriptive = table_name_descriptive,
                       check_for_deleted_rows = check_for_deleted_rows)
                      
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  
  #Archive all data files in the POLIS data folder
  archive_last_data(load_query_parameters()$table_name)
  
  #If field_name has changed then indicate re-pull
  #if a row with the table_name exists within cache, then pull the values from that row
  field_name_change <- NULL
  field_name_change <- load_query_parameters()$field_name != load_query_parameters()$prior_field_name
  
  #If a re-pull was indicated due to a field_name change, then reset the cache to initiate:
  polis_re_pull_cache_reset(table_name = load_query_parameters()$table_name,
                            field_name = load_query_parameters()$field_name,
                            re_pull_polis_indicator = field_name_change)
  #Create an array of API URLs
  urls <- create_url_array_combined(table_name = load_query_parameters()$table_name,
                                    min_date = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin),
                                    field_name = load_query_parameters()$field_name,
                                    download_size = load_query_parameters()$download_size,
                                    id_vars = load_query_parameters()$id_vars,
                                    method = NULL)
  query_output <- call_urls_combined(urls = urls,
                                     type = "full")
  new_table_metadata <- NULL
  if(!is.null(query_output) & nrow(query_output) != 0){
    new_table_metadata <- get_polis_metadata(query_output = query_output,
                                             table_name = load_query_parameters()$table_name)
  }
  
  #If a version of the data table has been previously pulled and saved, read it in and summarize it's metadata for comparison
  old_table_metadata <- NULL
  if(file.exists(file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name, ".rds"))) == TRUE){
    old_table_metadata <- get_polis_metadata(query_output = readRDS( file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name, ".rds"))),
                                             table_name = load_query_parameters()$table_name)
  }
  
  re_pull_polis_indicator <- FALSE
  #If both old and new metadata summaries exist, then compare them. If there are differences, then set the re_pull_polis_indicator to trigger re-pull of entire table
  if(!is.null(old_table_metadata) &
     !is.null(new_table_metadata)){
    re_pull_polis_indicator <- metadata_comparison(new_table_metadata = new_table_metadata,
                                                   old_table_metadata = old_table_metadata)$re_pull_polis_indicator
  }
  polis_re_pull_cache_reset(table_name = load_query_parameters()$table_name,
                            field_name = load_query_parameters()$field_name,
                            re_pull_polis_indicator = re_pull_polis_indicator)
  
  if(re_pull_polis_indicator == TRUE){
    urls <- create_url_array_combined(table_name = load_query_parameters()$table_name,
                                      min_date = as.Date("1900-01-01"),
                                      field_name = load_query_parameters()$field_name,
                                      download_size = load_query_parameters()$download_size,
                                      id_vars = load_query_parameters()$id_vars,
                                      method = NULL)
    query_output <- call_urls_combined(urls = urls,
                                       type = "re-pull")
  }
  
  #Combine the query output with the old dataset and save
  #Get a list of all obs id_vars in the full table (for removing deletions in append_and_save)
  full_idvars_output <- data.frame(matrix(nrow=0, ncol=0))
  if(check_for_deleted_rows == TRUE){
    urls <- create_url_array_combined(table_name = load_query_parameters()$table_name,
                                      id_vars = load_query_parameters()$id_vars,
                                      method="id_only")
    full_idvars_output <- call_urls_combined(urls, type="id_only")
  }
  
  #Combine the newly pulled dataset with the old dataset and save to folder
  new_query_output <- append_and_save(query_output = query_output,
                                      table_name = load_query_parameters()$table_name,
                                      id_vars = load_query_parameters()$id_vars,
                                      full_idvars_output = full_idvars_output)
  
  #Get the cache dates for the newly saved table
  update_cache_dates <- get_update_cache_dates(query_output = new_query_output,
                                               field_name = load_query_parameters()$field_name,
                                               table_name = load_query_parameters()$table_name)
  
  #Update the cache date fields
  update_cache(.file_name = load_query_parameters()$table_name,
               .val_to_update = "latest_date",
               .val = as.Date(update_cache_dates$latest_date), 
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  update_cache(.file_name = load_query_parameters()$table_name,
               .val_to_update = "updated",
               .val = as.POSIXct(update_cache_dates$updated, tz=Sys.timezone(), origin=lubridate::origin),
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  update_cache(.file_name = load_query_parameters()$table_name,
               .val_to_update = "date_field",
               .val = load_query_parameters()$field_name,
               cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')
  )
  
  #add cache data as attributes to rds
  add_cache_attributes(load_query_parameters()$table_name) 
  
  #Get change summary comparing final file to latest archived file
  change_summary <- compare_final_to_archive(load_query_parameters()$table_name,
                                             load_query_parameters()$id_vars,
                                             categorical_max = 30)
  #Save change_summary to cache
  save_change_summary(table_name = load_query_parameters()$table_name, 
                      change_summary = change_summary,
                      change_log_folder = NULL,
                      n_change_log = 30)
}
