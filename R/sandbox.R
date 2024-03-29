init_polis_data_struc(folder = "C:/Users/wxf7/Desktop/POLIS_data")

urls_skip_top <- create_url_array_combined("Synonym",field_name = "CreatedDate", method = "skip-top", download_size = 50)
url_id_method <- create_url_array_combined("Synonym",field_name = "CreatedDate")


get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Synonym",
                field_name = "CreatedDate",
                id_vars = "Id",
                download_size = 1000)

# Examples of get_polis_table:
get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Lqas",
                field_name = "None",
                id_vars = "Id",
                download_size = 1000)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "EnvSample",
                field_name = "LastUpdateDate",
                id_vars = "Id",
                download_size = 1000)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Activity",
                field_name = "ActivityDateTo",
                id_vars = "Id",
                download_size = 500)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "IndicatorValue('AFP_DOSE_0')",
                field_name = "LastUpdateDate",
                id_vars = "Id",
                download_size = 500)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "RefData('IndicatorCategories')",
                field_name = "None",
                id_vars = "Id",
                download_size = 500)

start <- Sys.time()
get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "EnvSample",
                field_name = "None",
                id_vars = "Id",
                download_size = 1000)
stop <- Sys.time()
query_time <- difftime(stop, start)


get_polis_table(table_name = "Lqas",
                field_name = "Start",
                id_vars = "Id",
                download_size = 1000)

get_polis_table()

get_polis_table(folder="//cdc.gov/project/CGH_GID_Active/PEB/SIR/DATA/POLIS",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d")

start_time <- Sys.time()
get_polis_data(folder="//cdc.gov/project/CGH_GID_Active/PEB/SIR/DATA/POLIS",
               token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
               dev=TRUE)
end_time <- Sys.time()
full_pull_time <- difftime(end_time, start_time, units=c("mins"))
print(full_pull_time)

get_polis_data(folder="C:/Users/wxf7/Desktop/POLIS_data",
               dev = TRUE)


test <- clean_polis_data(EnvSample)



# my_url <- "https://extranet.who.int/polis/api/v2/Virus?%24filter=SubRegionName%20ne%20null&%24inlinecount=allpages&token=BRfIZj%2FI9B3MwdWKtLzG%2BkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2BGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2BAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3D&skip=10000"
# my_url <- "https://extranet.who.int/polis/api/v2/EnvSample?&%24inlinecount=allpages&token=BRfIZj%2FI9B3MwdWKtLzG%2BkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2BGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2BAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3D"

my_url <- "https://extranet.who.int/polis/api/v2/RefData('YesNo')?&%24inlinecount=allpages&token=BRfIZj%2FI9B3MwdWKtLzG%2BkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2BGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2BAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3D&skip=10000"
test1 <- httr::GET(my_url)
# test1$status_code
test2 <- test1 %>% httr::content(type='text', encoding = 'UTF-8') %>% jsonlite::fromJSON() %>% {.$value} %>% as_tibble() %>% mutate_all(., as.character)
# table(test2$SubRegionName)
View(test2)


table(test2$Periodicity)
# table(test2$Code)
table(test2$PeriodType)
table(test2$Comments)


#tryCatch() example:
test_fx <- function(y){tryCatch({return(1 / y)}, error=function(e){
  y <- 10
  return(test_fx(y))
  })}
vec <- list(1, 2, 3, 0, "x", 4, 5)
out <- lapply(vec, test_fx)



start_time <- Sys.time()
get_polis_data(folder="C:/Users/ynm2/POLIS",
               token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
               dev=TRUE)
end_time <- Sys.time()
full_pull_time <- difftime(end_time, start_time, units=c("mins"))
print(full_pull_time)

source(here("R/polis_api_etl-package.R"), echo=TRUE)
source(here("R/cache_archive_snapshot_L2.R"), echo=TRUE)
source(here("R/create_url_array_L2.R"), echo=TRUE)
source(here("R/data_cleaning_L1.R"), echo=TRUE)
source(here("R/data_cleaning_L2.R"), echo=TRUE)
source(here("R/get_polis_data_L1.R"), echo=TRUE)
source(here("R/get_polis_table_L1.R"), echo=TRUE)
source(here("R/metadata_comparison_L2.R"), echo=TRUE)
source(here("R/process_and_call_urls_L2.R"), echo=TRUE)
source(here("R/set_up_structure_and_get_parameters_L2.R"), echo=TRUE)
source(here("R/utils.R"), echo=TRUE)


get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "UpdatedDate",
                id_vars = "Id",
                download_size = 1000,
                check_for_deleted_rows = FALSE)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "UpdatedDate",
                id_vars = "Id",
                download_size = 1000,
                check_for_deleted_rows = FALSE)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "None",
                id_vars = "Id",
                download_size = 1000,
                check_for_deleted_rows = FALSE)

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "UpdatedDate",
                id_vars = "Id",
                download_size = 1000,
                check_for_deleted_rows = FALSE)

#Mock metadata change (e.g. add a variable in Geography)
Geography <- readRDS("C:/Users/wxf7/Desktop/POLIS_data/Geography.rds")
Geography <- Geography %>%
  select(1:30) %>% 
  mutate(Name = ifelse(Name == "ETHIOPIA", "MISSPELLED", Name)) %>%
  filter(Name != "UGANDA")

write_rds(Geography, "C:/Users/wxf7/Desktop/POLIS_data/Geography.rds")

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "None",
                id_vars = "Id",
                download_size = 1000,
                check_for_deleted_rows = FALSE)
 
