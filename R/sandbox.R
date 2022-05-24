init_polis_data_struc(folder = "C:/Users/wxf7/Desktop/POLIS_data")

urls <- create_url_array("Lqas",field_name = "Start")


lqas <- pb_mc_api_pull(urls)
