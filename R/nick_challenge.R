#1) Step through the functions and get a general understanding of the flow of this package 

#2) Pick a table that you want to work with (smaller = Geography, larger = Virus)

#3) Recreate the api pull function 
#3.1) Takes as input - table name, api_specifications (token, folder name etc)
#3.2) Checks to see if we have table cached locally and if so what the latest update was
#      (lets pretend this is brand new and you don't have any data locally)
#3.3) Basic table metadata checking - verify that table exists in POLIS -> gets count of table -> gets variable names 
#3.4) Structure a URL request using skip / top to pull entire dataset for variables of interest

#4) Create a graph which looks at time to capture entire database as we vary the size of the request and the # of variables

tick <- Sys.time()
x <- pb_mc_api_pull(urls)
tock <- Sys.time()
diff <- tock - tick


#' @description function to evaluate api pull times 
#' @param table_name str describing table 
#' @param size size ofpull from api 
#' @param vars_of_interst str array of variables that we want to pull from table
#' @returns tibble with parameter specifications and time
nicks_function <- function(table_name, size, vars_of_interest){
  tibble(table = table_name, request_size = size, vars = vars_of_interst, diff)
}

expand.grid(
  "table_name" = c("Geography", "Virus"), 
  "size" = c(50, seq(100, 2000, 100))
)