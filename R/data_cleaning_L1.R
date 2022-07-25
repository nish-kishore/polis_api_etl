#' Clean POLIS data
#' 
#' Passes an input dataframe through generic cleaning functions including:
#' 1. Standardizing names of variables
#' 2. Guessing the appropriate class for each variable and assigning that class to the variable
#' 3. Removing any empty rows from the dataframe
#' 4. Changing any blanks or empty space observations to NA
#' 5. Replacing non-ASCII characters with ASCII approximation
#' 6. Removing exact duplicate rows
#' 
#' @param input_dataframe Dataframe to be cleaned
#' @return A cleaned version of input_dataframe
#' @export

clean_polis_data <- function(input_dataframe = NULL){
  input_dataframe <- cleaning_var_names_initial(input_dataframe)
  input_dataframe <- cleaning_var_class_initial(input_dataframe)
  input_dataframe <- cleaning_blank_to_na(input_dataframe)
  input_dataframe <- cleaning_remove_empty_rows(input_dataframe)
  input_dataframe <- cleaning_replace_special(input_dataframe)
  input_dataframe <- cleaning_dedup(input_dataframe)
  input_dataframe <- cleaning_var_class_initial(input_dataframe) #running cleaning_var_class_initial a second time converts character to logical 
  return(input_dataframe)
}
