init_polis_data_struc(folder = "C:/Users/wxf7/Desktop/POLIS_data")

urls <- create_url_array("Synonym",field_name = "CreatedDate")


pb_mc_api_pull(urls)

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
get_polis_data(folder="C:/Users/wxf7/Desktop/Full_POLIS_data",
               token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
               dev=TRUE)
end_time <- Sys.time()
full_pull_time <- difftime(end_time, start_time, units=c("mins"))
print(full_pull_time)

source("~/GitHub/polis_api_etl/R/polis_api_etl-package.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/cache_archive_snapshot_L2.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/create_url_array_L2.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/data_cleaning_L1.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/data_cleaning_L2.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/get_polis_data_L1.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/get_polis_table_L1.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/metadata_comparison_L2.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/process_and_call_urls_L2.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/set_up_structure_and_get_parameters_L2.R", echo=TRUE)
source("~/GitHub/polis_api_etl/R/utils.R", echo=TRUE)


get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d",
                table_name = "Geography",
                field_name = "None",
                id_vars = "Id",
                download_size = 1000)


