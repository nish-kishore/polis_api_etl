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
}

# Calculates new last-update and latest-date and enters it into the cache, saves the dataset as rds
get_update_cache_dates <- function(query_output,
                                   field_name = load_query_parameters()$field_name,
                                   table_name = load_query_parameters()$table_name){
  
  #If the newly pulled dataset contains any data, then pull the latest_date as the max of field_name in it
  if(!is.null(query_output) && nrow(query_output) > 0 && field_name %in% colnames(query_output)){
    temp <- query_output %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d")) %>%
      as.data.frame()
    latest_date <- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
  }
  
  #If the newly pulled dataset is empty (i.e. there is no new data since the last pull), then pull the latest_date as the max of field_name in the old dataset
  if((is.null(query_output) | nrow(query_output) == 0) && field_name %in% colnames(read_lines_raw(file.path(load_specs()$polis_data_folder,
                                                                                                            paste0(table_name, ".rds")),n_max=100))){
    old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
      mutate_all(.,as.character)
    temp <- old_polis %>%
      select(all_of(field_name)) %>%
      rename(field_name = 1) %>%
      mutate(field_name = as.Date(field_name, "%Y-%m-%d")) %>%
      as.data.frame()
    latest_date <- as.Date(max(temp[,1], na.rm=TRUE), "%Y-%m-%d")
  }
  #If field_name is blank or not in the dataset, save latest_date as NA
  if(!is.null(query_output) && nrow(query_output) > 0 && !(field_name %in% colnames(query_output))){
    latest_date <- NA
  }
  #Save the current system time as the date/time of 'updated' - the date the database was last checked for updates
  updated <- Sys.time()
  update_cache_dates <- as.data.frame(bind_cols(latest_date = latest_date, updated = updated))
  return(update_cache_dates)
}

#Function that moves the rds files in the polis_data folder to an archive folder
archive_last_data <- function(table_name = load_query_parameters()$table_name,
                              archive_folder = NULL, #folder pathway where the datasets will be archived
                              n_archive = 3 #Number of most-recent datasets to save in archive, per table
){
  #If archive_folder was not specified, then check if the default exists, if not then create it
  if(is.null(archive_folder)){
    archive_folder = file.path(load_specs()$polis_data_folder,"archive")
    if(file.exists(archive_folder) == FALSE){
      dir.create(archive_folder)
    }
  }
  
  #Get list of rds files to archive from polis_data_folder
  # current_files <- list.files(load_specs()$polis_data_folder) %>%
  #   stringr::str_subset(., pattern=".rds") %>%
  #   stringr::str_remove(., pattern=".rds")
  
  current_files <- table_name #Revised to just the current file (i.e. table name) so that the archive function can be placed in get_polis_table() instead of get_polis_data()
  
  #for each item in current_files list, check if an archive subfolder exists, and if not then create it
  # for(i in current_files){  
  
  if(file.exists(file.path(archive_folder, table_name)) == FALSE){
    dir.create(file.path(archive_folder, table_name))
  }
  #for each item in current_files:
  #delete the oldest file in it's subfolder if there are >= n_archive files in the subfolder
  archive_list <- list.files(file.path(archive_folder, table_name)) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  
  #if archive_list is not empty then get timestamps
  if(length(archive_list) > 0){
    archive_list_timestamp <- c()
    for(j in archive_list){
      timestamp <- as.POSIXct(file.info(file.path(archive_folder, table_name, paste0(j,".rds")))$ctime)
      archive_list_timestamp <- as.POSIXct(c(archive_list_timestamp, timestamp), origin=lubridate::origin)
    }
    oldest_file <- (bind_cols(file=archive_list, timestamp=archive_list_timestamp) %>%
                      mutate(timestamp   = as.POSIXct(timestamp)) %>%
                      arrange(timestamp) %>%
                      slice(1))$file %>%
      paste0(., ".rds")
  }
  if(length(archive_list) >= n_archive){
    invisible(file.remove(file.path(archive_folder, table_name, oldest_file)))
  }
  
  #write the current file to the archive subfolder
  
  if(file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name,".rds")))){
    current_file_timestamp <- attr(readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name,".rds"))), which="updated")
    current_file <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name,".rds")))
    write_rds(current_file, file.path(archive_folder, table_name, paste0(table_name, "_", format(as.POSIXct(current_file_timestamp), "%Y%m%d_%H%M%S_"),".rds")))
    #remove the current file from the main folder #Undid this as the current file is needed for append_and_save()
    # file.remove(paste0(load_specs()$polis_data_folder, "/", i,".rds"))
  }
  
}

#Add the metadata stored in cache to a table as an attribute, so that table-specific metadata can be retained through archiving / retrieval from archive
add_cache_attributes <- function(table_name = load_query_parameters()$table_name){
  #Get list of rds 
  # current_files <- list.files(load_specs()$polis_data_folder) %>%
  #   stringr::str_subset(., pattern=".rds") %>%
  #   stringr::str_remove(., pattern=".rds") 
  
  #For each rds, read it in, assign attributes from cache, and save it
  #get cache entry
  cache_entry <- read_cache(.file_name = table_name)
  #read in file
  file <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name,".rds")))
  #Assign attributes  
  attributes(file)$created <- cache_entry$created
  attributes(file)$date_field <- cache_entry$date_field
  attributes(file)$file_name <- cache_entry$file_name
  attributes(file)$file_type <- cache_entry$file_type
  attributes(file)$latest_date <- cache_entry$latest_date
  attributes(file)$updated <- cache_entry$updated
  #Write file
  write_rds(file, file.path(load_specs()$polis_data_folder, paste0(table_name,".rds")))
}

#Function that moves the rds files in the polis_data folder to an snapshot folder
save_snapshot <- function(snapshot_folder = NULL, #folder pathway where the datasets will be saved
                          snapshot_date = Sys.time()
){
  #If snapshot_folder was not specified, then check if the default exists, if not then create it
  if(is.null(snapshot_folder)){
    snapshot_folder = file.path(load_specs()$polis_data_folder,"snapshots")
    if(file.exists(snapshot_folder) == FALSE){
      dir.create(snapshot_folder)
    }
  }
  
  #Create snapshot subfolder
  x <- gsub(":","", snapshot_date)
  x <- gsub(" ","_", x)
  x <- gsub("-","", x)
  snapshot_subfolder <- file.path(snapshot_folder, paste0("snapshot_", x))
  dir.create(snapshot_subfolder)
  #Get list of rds files to save in snapshot from polis_data_folder
  current_files <- list.files(load_specs()$polis_data_folder) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  
  #for each item in current_files:
  
  #write the current file to the snapshot subfolder:
  for(i in current_files){
    write_rds(readRDS(file.path(load_specs()$polis_data_folder, paste0(i,".rds"))),
              file.path(snapshot_subfolder, paste0(i,".rds")))
  }
}

#Restore all datasets from the latest archived datasets with date at or prior to last_good_date
revert_from_archive <- function(last_good_date = Sys.Date()-1){
  folder <- load_specs()$polis_data_folder
  archive_folder <- file.path(load_specs()$polis_data_folder,"archive")
  
  #for each subfolder of archive_folder, get the file name/path of the most recent file created on/before last_good_date
  #get directory of subfolders
  subfolder_list <- list.files(archive_folder)
  #for each item in subfolder_list, get all file names then subset to most recent
  for(i in subfolder_list){
    subfolder_files <- list.files(file.path(archive_folder, i))
    if(length(subfolder_files) > 0){
      file_dates <- c()
      for(j in subfolder_files){
        file_date <- attr(readRDS(file.path(archive_folder, i, j)),which="updated")
        file_dates <- c(file_dates, file_date)
      }
      file_to_keep <- (bind_cols(name = subfolder_files, create_date = file_dates) %>%
                         mutate(create_date = as.POSIXct(create_date, origin = lubridate::origin)) %>%
                         filter(create_date <= as.POSIXct(paste0(last_good_date, " 23:59:59"), format="%Y-%m-%d %H:%M:%S", origin = lubridate::origin)) %>%
                         arrange(desc(create_date)) %>%
                         slice(1))$name
      #load file to keep
      if(length(file_to_keep) > 0){
        file_to_keep <- readRDS(file.path(archive_folder, i, file_to_keep))
        #write file to keep to data folder
        write_rds(file_to_keep, file.path(folder, paste0(i,".rds")))
      }
      if(length(file_to_keep) == 0){
        warning(paste0("There is no ", i, " table with an acceptable date in the archive. Current file retained."))
      }
    }
  }
  #Update the cache to reflect the reverted files
  update_cache_from_files()
}

update_cache_from_files <- function(){
  #read in cache
  cache <- readRDS(file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')) 
  
  #update the created, updated, and latest dates in the cache
  #get list of rds files  
  current_files <- list.files(load_specs()$polis_data_folder) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  #For each rds, read its attributes, assign attributes to cache
  for(i in current_files){  
    #read attributes  
    attr_created <- attr(readRDS(file.path(load_specs()$polis_data_folder,paste0(i,".rds"))), which = "created") 
    attr_date_field <- attr(readRDS(file.path(load_specs()$polis_data_folder, paste0(i,".rds"))), which = "date_field") 
    attr_file_type <- attr(readRDS(file.path(load_specs()$polis_data_folder,paste0(i,".rds"))), which = "file_type") 
    attr_latest_date <- attr(readRDS(file.path(load_specs()$polis_data_folder,paste0(i,".rds"))), which = "latest_date") 
    attr_updated <- attr(readRDS(file.path(load_specs()$polis_data_folder,paste0(i,".rds"))), which = "updated") 
    cache <- cache %>%
      mutate(created = as.POSIXct(ifelse(file_name == i, attr_created, created), format=("%Y-%m-%d %H:%M:%S"), origin = lubridate::origin),
             date_field = ifelse(file_name == i, attr_date_field, date_field),
             file_type = ifelse(file_name == i, attr_file_type, file_type),
             latest_date = as.Date(ifelse(file_name == i, attr_latest_date, latest_date), format=("%Y-%m-%d"), origin=lubridate::origin),
             updated = as.POSIXct(ifelse(file_name == i, attr_updated, updated), format=("%Y-%m-%d %H:%M:%S"), origin = lubridate::origin))
  }
  #Write revised cache
  write_rds(cache, file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds'))
}

#If re_pull_polis_indicator is TRUE, then re-pull the complete table and update the cache

polis_re_pull_cache_reset <- function(table_name = load_query_parameters()$table_name,
                          field_name = load_query_parameters()$field_name,
                          re_pull_polis_indicator,
                          replace_table){
  
  if(re_pull_polis_indicator == TRUE | replace_table ==TRUE){
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
      
      #delete previous rds
      if(file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
        invisible(file.remove(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds"))))
      }
    }
    # updated <- read_table_in_cache_dir(load_query_parameters()$table_name)$updated
    # latest_date <- read_table_in_cache_dir(load_query_parameters()$table_name)$latest_date
    # if(is.na(latest_date)){latest_date <- "1900-01-01"}
    updated <- Sys.time()
    latest_date <- "1900-01-01"
    
    #read in yaml
    query_parameters_yaml <- load_query_parameters()
    #modify yaml
    query_parameters_yaml$updated <- updated
    query_parameters_yaml$latest_date <- latest_date
    query_parameters_yaml$prior_field_name <- field_name
    #save yaml
    write_yaml(query_parameters_yaml, file.path(load_specs()$polis_data_folder,'cache_dir','query_parameters.yaml'))
  }
}
