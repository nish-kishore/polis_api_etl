compare_final_to_archive <- function(table_name = load_query_parameters()$table_name,
                                     id_vars = load_query_parameters()$id_vars,
                                     categorical_max = 30){
  id_vars <- as.vector(id_vars)
  #Load new_file
  new_file <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))

  #load latest file in archive subfolder
  archive_subfolder <- file.path(load_specs()$polis_data_folder, "archive", table_name)

  #for each item in subfolder_list, get all file names then subset to most recent
  subfolder_files <- list.files(paste0(archive_subfolder))
  file_dates <- c()
  for(j in subfolder_files){
    file_date <- attr(readRDS(file.path(archive_subfolder, j)),which="updated")
    file_dates <- c(file_dates, file_date)
  }
  latest_file <- c()
  if(length(subfolder_files)>0){
    latest_file <- (bind_cols(name = subfolder_files, create_date = file_dates) %>%
                      mutate(create_date = as.POSIXct(create_date, origin = lubridate::origin)) %>%
                      arrange(desc(create_date)) %>%
                      slice(1))$name
  }
  change_summary <- NULL
  if(length(latest_file) > 0){
    #load latest_file
    latest_file <- readRDS(file.path(archive_subfolder, latest_file)) 

    #get metadata for latest file and new_file
    new_file_metadata <- get_polis_metadata(query_output = new_file,
                                            table_name = table_name,
                                            categorical_max = categorical_max)
    old_file_metadata <- get_polis_metadata(query_output = latest_file,
                                            table_name = table_name,
                                            categorical_max = categorical_max)

    change_summary <- metadata_comparison(new_file_metadata, old_file_metadata)[2:5]

    #count obs added to new_file and get set
    in_new_not_old <- new_file %>%
      anti_join(latest_file, by=as.vector(id_vars))

    #count obs removed from old_file and get set
    in_old_not_new <- latest_file %>%
      anti_join(new_file, by=as.vector(id_vars))

    #count obs modified in new file compared to old and get set
    in_new_and_old_but_modified <- new_file %>%
      select(-c(setdiff(colnames(new_file), colnames(latest_file)))) %>%
      setdiff(., latest_file %>%
                select(-c(setdiff(colnames(latest_file), colnames(new_file)))))
    if(nrow(in_new_and_old_but_modified) > 0){
      in_new_and_old_but_modified <- in_new_and_old_but_modified %>%
        inner_join(latest_file %>%
                     select(-c(setdiff(colnames(latest_file), colnames(new_file)))) %>%
                     setdiff(., new_file %>%
                               select(-c(setdiff(colnames(new_file), colnames(latest_file))))), by=as.vector(id_vars)) %>%
        #wide_to_long
        pivot_longer(cols=-id_vars) %>%
        mutate(source = ifelse(str_sub(name, -2) == ".x", "new", "old")) %>%
        mutate(name = str_sub(name, 1, -3)) %>%
        #long_to_wide
        pivot_wider(names_from=source, values_from=value)
      
      
      if("new" %in% colnames(in_new_and_old_but_modified) &
         "old" %in% colnames(in_new_and_old_but_modified)){
        in_new_and_old_but_modified <- in_new_and_old_but_modified %>%
          rowwise() %>%
          mutate(new = paste(unlist(new),collapse=", ")) %>%
          mutate(old = paste(unlist(old),collapse=", ")) %>%
          filter(new != old)
      }
    }
    if(nrow(in_new_and_old_but_modified) == 0){
      in_new_and_old_but_modified <- data.frame(matrix(ncol=4, nrow=0))
      colnames(in_new_and_old_but_modified) <- c("Id", "name", "new", "old")
    }

    #summary counts
    n_added <- nrow(in_new_not_old)
    n_edited <- nrow(in_new_and_old_but_modified %>%
                       select(id_vars) %>%
                       unique())
    n_deleted <- nrow(in_old_not_new)
    obs_change <- c(n_added = n_added,
                    n_edited = n_edited,
                    n_deleted = n_deleted)

    #add summary to change_summary along with datasets
    change_summary <- append(change_summary,
                             list(obs_change = obs_change,
                                  obs_added = in_new_not_old,
                                  obs_edited = in_new_and_old_but_modified,
                                  obs_deleted = in_old_not_new))

  }
  return(change_summary)
}



save_change_summary <- function(table_name = load_query_parameters()$table_name,
                                change_summary,
                                change_log_folder = NULL,
                                n_change_log = 30){
  #If change_log_folder was not specified, then check if the default exists, if not then create it
  if(is.null(change_log_folder)){
    change_log_folder = file.path(load_specs()$polis_data_folder,"change_log")
    if(file.exists(change_log_folder) == FALSE){
      dir.create(change_log_folder)
    }
  }

  #If change_log subfolder does not exist, then create it
  change_log_subfolder = file.path(load_specs()$polis_data_folder,"change_log", table_name)
  if(file.exists(change_log_subfolder) == FALSE){
    dir.create(change_log_subfolder)
  }

  #delete the oldest file in subfolder if there are >= n_change_log files in the subfolder
  change_log_list <- list.files(change_log_subfolder) %>%
    stringr::str_subset(., pattern=".rds") %>%
    stringr::str_remove(., pattern=".rds")
  change_log_list_timestamp <- c()
  for(j in change_log_list){
    timestamp <- as.POSIXct(file.info(file.path(change_log_subfolder, paste0(j,".rds")))$ctime)
    change_log_list_timestamp <- as.POSIXct(c(change_log_list_timestamp, timestamp), origin=lubridate::origin)
  }
  if(length(change_log_list) > 0){
    oldest_file <- (bind_cols(file=change_log_list, timestamp=change_log_list_timestamp) %>%
                      mutate(timestamp   = as.POSIXct(timestamp)) %>%
                      arrange(timestamp) %>%
                      slice(1))$file %>%
      paste0(., ".rds")
  }
  if(length(change_log_list) >= n_change_log){
    invisible(file.remove(file.path(change_log_subfolder, oldest_file)))
  }
  #write the current file to the archive subfolder
  write_rds(change_summary, file.path(change_log_subfolder, paste0(table_name, "_change_log_", format(as.POSIXct(Sys.time()), "%Y%m%d_%H%M%S_"),".rds")))
}

#function which prints the latest change log into console
print_latest_change_log_summary <- function(){
  #load and combine latest change_log for all tables
  #Check if change_log folder exists. If not, go to end.
  change_log_folder <- file.path(load_specs()$polis_data_folder,"change_log")
  if(file.exists(change_log_folder) == TRUE){
    #Get list of subfolders
    change_log_subfolder_list <- list.files(change_log_folder)
    #Get list of latest file name within each change_log_subfolder
    change_log_file_list <- c()
    obs_change_combined <- data.frame()
    class_changed_vars_combined <- data.frame()
    new_response_combined <- data.frame()
    lost_vars_combined <- data.frame()
    new_vars_combined <- data.frame()
    for(i in change_log_subfolder_list){
      change_log_list <- c()
      change_log_subfolder <- file.path(change_log_folder, i)
      change_log_list <- list.files(change_log_subfolder) %>%
        stringr::str_subset(., pattern=".rds") %>%
        stringr::str_remove(., pattern=".rds")
      change_log_list_timestamp <- c()
      for(j in change_log_list){
        timestamp <- as.POSIXct(file.info(file.path(change_log_subfolder, paste0(j,".rds")))$ctime)
        change_log_list_timestamp <- as.POSIXct(c(change_log_list_timestamp, timestamp), origin=lubridate::origin)
      }
      if(length(change_log_list) > 0){
        newest_file <- (bind_cols(file=change_log_list, timestamp=change_log_list_timestamp) %>%
                          mutate(timestamp   = as.POSIXct(timestamp)) %>%
                          arrange(desc(timestamp)) %>%
                          slice(1))$file %>%
          paste0(., ".rds")
        change_log <- readRDS(file.path(change_log_subfolder, newest_file))
        new_response <- change_log$new_response %>%
          mutate(table_name = i)
        class_changed_vars <- change_log$class_changed_vars %>%
          mutate(table_name = i)
        if(length(change_log$lost_vars) > 0){
          lost_vars <- c(i, paste(change_log$lost_vars, ","))
        }
        if(length(change_log$lost_vars) == 0){
          lost_vars <- c()
        }
        if(length(change_log$lost_vars) > 0){
          new_vars <- c(i, paste(change_log$new_vars, ","))
        }
        if(length(change_log$new_vars) == 0){
          new_vars <- c()
        }
        obs_change <- as.data.frame(change_log$obs_change)
        obs_change$change <- rownames(obs_change)
        obs_change <- obs_change %>%
          pivot_wider(names_from = change, values_from=`change_log$obs_change`) %>%
          mutate(table_name = i)

        obs_change_combined <- obs_change_combined %>%
          bind_rows(obs_change)
        class_changed_vars_combined <- class_changed_vars_combined %>%
          bind_rows(class_changed_vars)
        new_response_combined <- new_response_combined %>%
          bind_rows(new_response)
        lost_vars_combined <- lost_vars_combined %>%
          rbind(lost_vars)
        if(ncol(lost_vars_combined) == 2){colnames(lost_vars_combined) <- c("table_name", "lost_vars")}
        new_vars_combined <- new_vars_combined %>%
          rbind(new_vars)
        if(ncol(new_vars_combined) == 2){colnames(new_vars_combined) <- c("table_name", "new_vars")}
      }
    }
    #for each type of change, filter to where there are changes if needed
    obs_change_combined %>%
      rowwise() %>%
      mutate(tot_change = sum(n_added, n_edited, n_deleted)) %>%
      ungroup() %>%
      filter(tot_change > 0) %>%
      select(table_name, n_added, n_edited, n_deleted)

    #print summary of each type of change
    if(nrow(new_vars_combined) > 0){
      print("New variables were found in the following tables:")
      new_vars_combined
    } else {print("No new variables were found in any table since last download.")}

    if(nrow(lost_vars_combined) > 0){
      print("Variables were dropped from the following tables:")
      lost_vars_combined
    } else {print("No variables were dropped from any table since last download.")}

    if(nrow(new_response_combined) > 0){
      print("New categorical responses were found in the following tables/variables:")
      new_response_combined %>% select(table_name, var_name, in_new_not_old)
    } else {print("No new categorical responses were found in any table since last download.")}

    if(nrow(class_changed_vars_combined) > 0){
      print("Changes in variable classes were found in the following tables:")
      class_changed_vars_combined %>% select(table_name, var_name, old_var_class, new_var_class)
    } else {print("No changes in variable classes were found in any table since last download.")}

    if(nrow(obs_change_combined) > 0){
      print("The following counts of observations were added/edited/deleted from each table since the last download:")
      obs_change_combined
    } else {print("No observation additions/edits/deletions were found in any table since last download.")}
  }
}
#Summarise POLIS metadata and store in cache
get_polis_metadata <- function(query_output,
                               table_name = load_query_parameters()$table_name,
                               categorical_max = 30){
  if(nrow(query_output)>0){
    #summarise var names and classes
    var_name_class <- skimr::skim(query_output) %>%
      select(skim_type, skim_variable, character.n_unique) %>%
      rename(var_name = skim_variable,
             var_class = skim_type)

    #categorical sets: for categorical variables with <= n unique values, get a list of unique values
    categorical_vars <- query_output %>%
      select(var_name_class$var_name[var_name_class$character.n_unique <= categorical_max]) %>%
      pivot_longer(cols=everything(), names_to="var_name", values_to = "response") %>%
      distinct() %>%
      pivot_wider(names_from=var_name, values_from=response, values_fn = list) %>%
      pivot_longer(cols=everything(), names_to="var_name", values_to="categorical_response_set")

    #Combine var names/classes/categorical-sets into a 'metadata table'
    table_metadata <- var_name_class %>%
      select(-character.n_unique) %>%
      left_join(categorical_vars, by=c("var_name"))
  }
  if(nrow(query_output) == 0){
    table_metadata <- NULL
  }
  return(table_metadata)
}

#Compare metadata of newly pulled dataset to cached metadata
metadata_comparison <- function(new_table_metadata,
                                old_table_metadata,
                                verbose=TRUE){
  #if new or old metadata are null, go to end
  if(!is.null(new_table_metadata) & !is.null(old_table_metadata)){
    if(nrow(new_table_metadata) != 0 & nrow(old_table_metadata) != 0){
      #compare to old metadata
      compare_metadata <- old_table_metadata %>%
        full_join(new_table_metadata, by=c("var_name"))

      #Get list of new variables (variables in the new dataset but not the old)
      new_vars <- (compare_metadata %>%
                     filter(is.na(var_class.x)))$var_name

      if(length(new_vars) != 0){
        new_vars <- new_vars
        if(verbose == TRUE){
          warning("There are new variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'new_vars'")
        }
      }

      #Get list of lost variables (variables in the old dataset but not the new)
      lost_vars <- (compare_metadata %>%
                      filter(is.na(var_class.y)))$var_name

      if(length(lost_vars) != 0){
        lost_vars <- lost_vars
        if(verbose == TRUE){
          warning("There are missing variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'lost_vars'")
        }
      }

      #Get list of variables that have a different class in the new dataset than in old dataset
      class_changed_vars <- compare_metadata %>%
        filter(!(var_name %in% lost_vars) &
                 !(var_name %in% new_vars) &
                 (var_class.x != var_class.y &
                    !is.null(var_class.x) & !is.null(var_class.y))) %>%
        select(-c(categorical_response_set.x, categorical_response_set.y)) %>%
        rename(old_var_class = var_class.x,
               new_var_class = var_class.y)

      if(nrow(class_changed_vars) != 0){
        class_changed_vars <- class_changed_vars
        if(verbose == TRUE){
          warning("There are variables in the POLIS table with different classes\ncompared to when it was last retrieved\nReview in 'class_changed_vars'")
        }
      }

      #Check for new responses in categorical variables (excluding new variables and class changed variables that have been previously shown)
      new_response <- compare_metadata %>%
        filter(!(var_name %in% lost_vars) &
                 !(var_name %in% new_vars) &
                 !(var_name %in% class_changed_vars$var_name) &
                 as.character(categorical_response_set.x) != "NULL" &
                 as.character(categorical_response_set.y) != "NULL") %>%
        rowwise() %>%
        mutate(same = toString(intersect(categorical_response_set.x, categorical_response_set.y)),
               in_old_not_new = toString(setdiff(categorical_response_set.x, categorical_response_set.y)),
               in_new_not_old = toString(setdiff(categorical_response_set.y, categorical_response_set.x))) %>%
        filter(in_new_not_old != "") %>%
        rename(old_categorical_response_set = categorical_response_set.x,
               new_categorical_response_set = categorical_response_set.y) %>%
        select(var_name, old_categorical_response_set, new_categorical_response_set, same, in_old_not_new, in_new_not_old)

      if(nrow(new_response) != 0){
        new_response <- new_response

        if(verbose == TRUE){
          warning("There are categorical responses in the new table\nthat were not seen when it was last retrieved\nReview in 'new_response'")
        }
      }

      #Create an indicator that is TRUE if there has been a change in table structure or content that requires re-pulling of the table
      re_pull_polis_indicator <- FALSE
      if(
        # nrow(new_response) != 0 |
        nrow(class_changed_vars) != 0 |
        # length(lost_vars) != 0 |
        length(new_vars) != 0){
        re_pull_polis_indicator <- TRUE
      }
    }
  }
  if(is.null(new_table_metadata) | is.null(old_table_metadata)){
    re_pull_polis_indicator <- FALSE
  }
  #Combine the changes into a list and return to be saved
  change_summary <- list(re_pull_polis_indicator = re_pull_polis_indicator, new_response = new_response, class_changed_vars = class_changed_vars, lost_vars = lost_vars, new_vars = new_vars)
  return(change_summary)
}
