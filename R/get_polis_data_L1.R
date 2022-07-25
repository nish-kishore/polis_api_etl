#' Get POLIS Data
#'
#' Extract and save all POLIS tables. For each table:
#' 1. If table was previously pulled, move previous version to archive folder
#' 2. Use cache entry to determine range of data pull
#' 3. Pull data from POLIS
#' 4. Compare pulled data metadata to previous pulled data metadata to determine if a full re-pull of the table is needed
#' 5. If re-pull is indicated, then re-pull the full table
#' 6. Update cache entry
#' 7. Save a summary of changes between new data pull and previous data pull
#' 
#' @param folder Folder pathway where POLIS data will be or are saved
#' @param token POLIS API token
#' @param verbose Indicator for whether or not to print progress messages to console while running
#' @param dev Indicator for whether or not to pull the limited set of tables used for testing / package development
#' @param check_for_deleted_rows Indicator for whether or not to check the full POLIS table for any deleted rows, and remove any of these from the locally saved version of the table
#' @export

get_polis_data <- function(folder = NULL,
                           token = "",
                           verbose=TRUE,
                           dev = FALSE,
                           check_for_deleted_rows = FALSE){
  
  
  #If folder location and token not provided, prompt user for input:
  
  if (is.null(folder) && interactive() && .Platform$OS.type == "windows"){
    folder <- utils::choose.dir(default = getwd(), caption = "Select a folder where POLIS data will be saved:")
  }
  
  #If folder structure and cache not created already, create them
  init_polis_data_struc(folder=folder, token=token)
  
  #If token not provided, check for it in cache. 
  if(load_specs()$polis$token == ""){
    # If not in cache, if the token was provided as a function parameter, use that, else prompt for token entry
    valid_token <- TRUE
    if(token == ""){
      token <- readline("Enter POLIS APIv2 Token: ")
    }  
    #Check if token is valid, stop if not
    valid_token <- validate_token(token)
    if(valid_token == FALSE){
      stop("Invalid token.")
    }
    #save new token to specs yaml
    specs_yaml <- file.path(folder,'cache_dir','specs.yaml')
    specs <- read_yaml(file.path(folder,'cache_dir','specs.yaml'))
    specs$polis$token <- token
    write_yaml(specs, specs_yaml)
    Sys.setenv("token" = token)
  }
  
  #Get default POLIS table names, field names, and download sizes
  if(dev == TRUE){
    # defaults <- load_defaults() %>%
    #   filter(grepl("RefData", table_name) | table_name == "Lqas") #Note: This filter is in place for development purposes - to reduce the time needed for testing. Remove for final
    defaults <- load_defaults() %>%
      filter(
        # grepl("RefData", table_name) &
        grepl("IndicatorValue", table_name) &
          !(table_name %in% c("Activity", "Case", "Virus"))) #Note: This filter is in place for development purposes - to reduce the time needed for testing. Remove for final
    
  }
  if(dev == FALSE){
    defaults <- load_defaults()
  }
  
  #run get_polis_table iteratively over all tables
  
  for(i in 1:nrow(defaults)){
    table_name <- defaults$table_name[i]
    field_name <- defaults$field_name[i]
    id_vars <- defaults$id_vars[i]
    download_size <- as.numeric(defaults$download_size[i])
    table_name_descriptive <- defaults$table_name_descriptive[i]
    print(paste0("Downloading ", table_name_descriptive," Table [", i,"/", nrow(defaults),"]"))
    get_polis_table(folder = load_specs()$polis_data_folder,
                    token = load_specs()$polis$token,
                    table_name = table_name,
                    field_name = field_name,
                    id_vars = id_vars,
                    download_size = download_size,
                    table_name_descriptive = table_name_descriptive)
  }
  
  cat(paste0("POLIS data have been downloaded/updated and are stored locally at ", folder, ".\n\nTo load all POLIS data please run load_raw_polis_data().\n\nTo review meta data about the cache run [load cache data function]\n"))
}





