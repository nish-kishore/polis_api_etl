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
                download_size = 500)

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

get_polis_table(folder="C:/Users/wxf7/Desktop/POLIS_data",
                token="BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d")

get_polis_data()
get_polis_data(folder="C:/Users/wxf7/Desktop/POLIS_data",
               dev = TRUE)


test <- clean_polis_data(EnvSample)


test <- clean_polis_data(Synonym)
