clean_polis_data <- function(input_dataframe = NULL){
  input_dataframe <- cleaning_var_names_initial(input_dataframe)
  input_dataframe <- cleaning_var_class_initial(input_dataframe)
  input_dataframe <- cleaning_remove_empty_rows(input_dataframe)
  input_dataframe <- cleaning_blank_to_na(input_dataframe)
  input_dataframe <- cleaning_replace_special(input_dataframe)
  input_dataframe <- cleaning_dedup(input_dataframe)
  input_dataframe <- cleaning_var_class_initial(input_dataframe) #running cleaning_var_class_initial a second time converts character to logical 
  return(input_dataframe)
}
