#' Cleaning: Guess and Assign Var Classes
#'
#' data cleaning: re-assign classes to each variable by the following rules (applied in order): [From: https:/r4ds.had.co.nz/data-import.html]
#' 1. logical: contains only "F", "T", "FALSE", or "TRUE"
#' 2. integer: contains only numeric characters (and -).
#' 3. double: contains only valid doubles (including numbers like 4.5e-5)
#' 4. number: contains valid doubles with the grouping mark inside.
#' 5. time: matches the default time_format
#' 6. date: matches the default date_format
#' 7. date-time: any ISO8601 date
#' 8. If none of the above apply, then keep col as character.
#' 
#' @param input_dataframe Dataframe to be cleaned, with all variables in character class
#' @return Dataframe with each variable converted into appropriate class when class could be guessed

cleaning_var_class_initial <- function(input_dataframe = NULL){
  if(!is.null(input_dataframe)){
    #Get 3 sets of 1000 rows to use to guess classes and compare
    #if <1000 rows, the full dataset will be used to guess classes (no need to compare)
    if(nrow(input_dataframe) > 1000){
      set1 <- input_dataframe %>%
        #Sample 1000 rows at random 
        sample_n(size=1000, replace=FALSE)
      class_set1 <- set1 %>%
        mutate_all( ~ guess_parser(.)) %>%
        unique() 
      
      set2 <- input_dataframe %>%
        head(1000)
      class_set2 <- set2 %>%
        mutate_all( ~ guess_parser(.)) %>%
        unique() 
      
      set3 <- input_dataframe %>%
        tail(1000)
      class_set3 <- set3 %>%
        mutate_all( ~ guess_parser(.)) %>%
        unique() 
      
      class_set <- class_set1 %>%
        bind_rows(class_set2) %>%
        bind_rows(class_set3) %>%
        unique()
      
      class_set_keep_character <- class_set %>%
        summarise_all(n_distinct) %>%
        t() %>%
        as.data.frame() %>%
        rename(number_of_class_guesses = V1) %>%
        filter(number_of_class_guesses > 1)
      
      class_set_keep_character$var_name <- row.names(class_set_keep_character)
      class_set <- class_set %>%
        head(1) %>%
        t() %>%
        as.data.frame() %>%
        rename(class = V1) 
      
      class_set$var_name <- row.names(class_set)
      class_set <- class_set %>% 
        mutate(class = ifelse(var_name %in% class_set_keep_character$var_name, "character", class))
      rownames(class_set) = NULL
      class_set <- class_set %>%
        pivot_wider(names_from = var_name, values_from=class)
    }
    if(nrow(input_dataframe <= 1000)){
      class_set <- input_dataframe %>%
        mutate_all( ~ guess_parser(.)) %>%
        unique() 
    }
    input_dataframe <- input_dataframe %>%
      mutate_at(colnames(class_set), parse_guess) 
    
    #Convert POSIXct to date when no time info is available
    posixct_dates <- input_dataframe %>%
      select_if(is.POSIXct) %>%
      mutate_all(., ~ifelse((strftime(as.POSIXlt(.), format="%H:%M:%S"))=="00:00:00", 0,1)) %>%
      summarise_all(max, na.rm=TRUE) %>%
      t() %>%
      as.data.frame() %>%
      rename(date_and_time = V1) %>%
      filter(date_and_time == 0) %>%
      t() %>%
      colnames()
    input_dataframe <- input_dataframe %>%
      mutate_at(posixct_dates, as.Date)
  }
  return(input_dataframe)
}

#' Cleaning: Deduplicate
#'
#' data cleaning: remove duplicates either across all vars, or a set of vars if specified by the user
#' 
#' @param input_dataframe Dataframe to be cleaned
#' @param dedup_vars A vector of variable names to be used as a unique ID. If repeats of this set are found, only the first row is retained.
#' @return Dataframe with exact duplicates removed

cleaning_dedup <- function(input_dataframe = NULL,
                           dedup_vars = NULL #a vector of var names to deduplicate on
){
  if(!is.null(input_dataframe)){
    if(is.null(dedup_vars)){
      input_dataframe <- input_dataframe %>%
        distinct(across(everything()))
    }
    if(!is.null(dedup_vars)){
      input_dataframe <- input_dataframe %>%
        distinct(across(as.vector(dedup_vars)))
    }
  }
  return(input_dataframe)
}

#' Cleaning: Remove Blank Rows
#'
#' data cleaning: remove blank rows, or remove all rows where all variables are "NA"
#' @param input_dataframe Dataframe to be cleaned
#' @return Dataframe with blank rows removed

cleaning_remove_empty_rows <- function(input_dataframe = NULL){
  if(!is.null(input_dataframe)){
    input_dataframe <- input_dataframe %>%
      janitor::remove_empty(which=c("rows"))
  }
  return(input_dataframe)
}


# #data cleaning: rename from file
# cleaning_var_names_from_file <- function(table_name = NULL,
#                                          input_dataframe = NULL,
#                                          var_name_file = NULL,
#                                          desired_naming_convention = NULL){
#   if(is.null(var_name_file)){
#     #Check if file exists. if not, then create the folder and download the file
#     if(file.exists(file.path(load_specs()$polis_data_folder, "datafiles", "var_name_synonyms.rds")) == FALSE){
#       if(file.exists(file.path(load_specs()$polis_data_folder, "datafiles")) == FALSE){
#         dir.create(file.path(load_specs()$polis_data_folder, "datafiles"))
#       }
#       var_name_synonyms <- read.csv("https:/raw.githubusercontent.com/nish-kishore/polis_api_etl/lbaertlein1-patch-1/datafiles/var_name_synonyms.rds?token=GHSAT0AAAAAABUCSLUVJW7PMDMWI37IARHYYVA4CHA")
#       write_rds(var_name_synonyms, file.path(load_specs()$polis_data_folder, "datafiles", "var_name_synonyms.rds"))
#     }
#     var_name_synonyms <- readRDS(file.path(load_specs()$polis_data_folder, "datafiles", "var_name_synonyms.rds"))
#   }
#   if(!is.null(var_name_file)){
#     var_name_synonyms <- readRDS(var_name_file)
#   }
#   
#   
#   table_var_name_synonyms <- var_name_synonyms %>%
#     filter(table_name == table_name) %>%
#     janitor::remove_empty(which=c("rows", "cols")) %>%
#     janitor::clean_names() %>%
#     select(-c("table_name"))
#   
#   if(is.null(desired_naming_convention)){
#     desired_naming_convention <- colnames(table_var_name_synonyms)[utils::menu(colnames(table_var_name_synonyms), title="Select a Variable Naming Convention:")]
#   }
#   original_to_desired <- var_name_synonyms %>%
#     select(original_var_name, {{desired_naming_convention}} )
#   for(i in 1:ncol(input_dataframe)){
#     original_name <- colnames(input_dataframe)[i]
#     new_name <- (original_to_desired %>%
#                    filter(original_var_name == {{original_name}}))[1,2]
#     input_dataframe <- input_dataframe %>%
#       rename({{new_name}} := {{original_name}})
#   }
#   return(input_dataframe)
# }

#' Cleaning: Get Classes From Metadata
#'
#' Get variable class from metadata page (needs some correction still to match metadata to API table names/var names)
#' @return Dataframe containing variable class data scraped from POLIS metadata page
cleaning_var_class_from_metadata <- function(){
  url <- 'https:/extranet.who.int/polis/api/v2/$metadata'
  
  nodes <- read_html(url, xpath = '/h3 | /*[contains(concat( " ", @class, " 
" ), concat( " ", "entry-title", " " ))]')
  
  page <- htmlTreeParse(nodes)$children[["html"]][["body"]][["edmx"]][["dataservices"]][["schema"]] %>%
    xmlToList(., simplify=TRUE, addAttributes = TRUE)
  
  metadata <- data.frame()
  for(i in 1:(length(page)-1)){
    table <- page[[i]][[".attrs"]][[1]]
    for(j in 2:(length(page[[i]])-1)){
      polis_name <- page[[i]][[j]][["name"]]
      polis_type <- page[[i]][[j]][["type"]]
      row <- c(table, polis_name, polis_type)
      metadata <- rbind(metadata, row) %>%
        rename(table = 1, polis_name = 2, polis_type = 3)
      print(paste0(i, ", ", j))
    }
  }  
  metadata <- metadata %>%
    mutate(table = str_replace_all(table, "ApiV2", ""),
           polis_type = str_replace_all(polis_type, "Edm.", ""),
           class = case_when(polis_type %in% c("Boolean") ~ "logical",
                             polis_type %in% c("DateTime") ~ "POSIXct",
                             polis_type %in% c("Decimal", "Double", "Int16", "Int32", "Int64") ~ "numeric",
                             polis_type %in% c("Guid", "String") ~ "character",
                             TRUE ~ NA_character_))
  return(metadata)
}

#' Cleaning: Convert Blank to NA
#' data cleaning: convert all empty or blank-space strings to NA
#' @param input_dataframe Dataframe to be cleaned
#' @return Dataframe with all empty or blank-space values converted to NA
cleaning_blank_to_na <- function(input_dataframe){
  input_dataframe <- input_dataframe %>%
    mutate_all(list(~str_trim(.))) %>% #remove all leading/trailing whitespaces as well as replace all " " with ""
    mutate_all(list(~na_if(.,""))) #replace "" with NA
}

#' Cleaning: Replace Special Characters
#'
#' data cleaning: replace all special or non-ASCII characters with alphanumeric
#' 
#' @param input_dataframe Dataframe to be cleaned
#' @return A dataframe with all non-ASCII characters in all fields replaced with ASCII approximation

cleaning_replace_special <- function(input_dataframe){
  #for each character variable, replace special characters
  input_dataframe <- input_dataframe %>%
    mutate_if(is.character, 
              stringi::stri_trans_general,
              id = "Latin-ASCII") 
  return(input_dataframe)
}

#' Cleaning: Var Names
#'
#' data cleaning: standardize variable names by the following rules: [From: https:/cran.r-project.org/web/packages/janitor/vignettes/janitor.html]
#' Parse letter cases and separators to a consistent format (e.g. snake_case)
#' Remove leading/trailing/repeating spaces
#' Replace special characters with alphanumeric (e.g. o to oe)
#' Append numbers to duplicated names
#' Convert '%' to 'percent' and '#' to number
#' 
#' @param input_dataframe Dataframe to have variable names cleaned
#' @param case Which case type to use in the standardized names (common options include "snake", "lower_camel", "upper_camel". Full set of options are further described in janitor package documentation here: https://www.rdocumentation.org/packages/janitor/versions/1.2.0/topics/clean_names
#' @return A dataframe with standardized variable names
cleaning_var_names_initial <- function(input_dataframe = NULL,
                                       case="snake"){
  if(!is.null(input_dataframe)){
    input_dataframe <- input_dataframe %>%
      janitor::clean_names(case=case) %>%
      rename_all(stringi::stri_trans_general,
                 id = "Latin-ASCII")
  }
  return(input_dataframe)
}
