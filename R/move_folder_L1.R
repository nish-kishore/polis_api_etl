#' Move Folder
#' 
#' Moves the full contents of a folder from one location to another, including subfolder content
#' 
#' @param from Pathway of source folder to be copied (including folder name)
#' @param to Pathway of new folder location (including folder name)
#' @param retain_old An indicator for whether or not the source folder should retained (TRUE) or deleted (FALSE)
#' @export
#' @examples 
#' \dontrun{
#' \donttest{
#' move_folder(from = "old_folder_pathway", to = "new_folder_pathway", retain_old = TRUE) ## moves folder at old_folder_pathway to new_folder_pathway
#' }
#' }
move_folder <- function(from, to, retain_old = TRUE) {
  #change yaml polis_data_folder
  specs_yaml <- file.path(to,'cache_dir','specs.yaml')
  specs <- read_yaml(file.path(from,'cache_dir','specs.yaml'))
  specs$polis_data_folder <- to
  
  #copy folder from old location to new
  if(retain_old == FALSE){
    file.rename(from, to)
  }
  if(retain_old == TRUE){
    R.utils::copyDirectory(from, to)
  }
  
  #write the new yaml
  file.remove(specs_yaml)
  write_yaml(specs, specs_yaml)
  #save the new folder in Sys Environment
  Sys.setenv("polis_data_folder" = to)
}

