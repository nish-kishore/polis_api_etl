#Function to move the full folder to a new location
  #from is old folder pathway, to is new folder pathway, retain_old is an indicator for whether or not the old folder should be retained or deleted
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

