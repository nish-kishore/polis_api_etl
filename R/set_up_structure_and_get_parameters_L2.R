#' Check to see if cache exists, if not create it
#' @param folder A string, the location of the polis data folder
#' @return A string describing the creation process or errors
init_polis_data_struc <- function(folder, token){
  #check to see if folder exists, if not create it
  if(!dir.exists(folder)){
    dir.create(folder)
  }
  #check to see if cache directory exists, if not create it
  cache_dir <- file.path(folder, "cache_dir")
  if(!dir.exists(cache_dir)){
    dir.create(cache_dir)
  }
  #check to see if cache admin rds exists, if not create it
  cache_file <- file.path(cache_dir, "cache.rds")
  if(!file.exists(cache_file)){
    tibble(
      "created"=Sys.time(),
      "updated"=Sys.time(),
      "file_type"="INIT",
      "file_name"="INIT",
      "latest_date"=as_date("1900-01-01"),
      "date_field" = "N/A"
    ) %>%
      write_rds(cache_file)
  }
  #create specs yaml
  specs_yaml <- file.path(folder,'cache_dir','specs.yaml')
  if(!file.exists(specs_yaml)){
    if(is.null(token)){token <- ''}
    yaml_out <- list()
    yaml_out$polis$token <- token
    yaml_out$polis_data_folder <- folder
    write_yaml(yaml_out, specs_yaml)
  }
  Sys.setenv("polis_data_folder" = folder)
  if(token != "" & !is.null(token)){
    Sys.setenv("token" = token)
  }
}

#Check to see if table exists in cache_dir. If not, last-updated and last-item-date are default min and entry is created; if table exists no fx necessary
init_polis_data_table <- function(table_name = table_name,
                                  field_name = field_name){
  folder <- load_specs()$polis_data_folder
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  #If there is no cache entry for the requested table then create it: 
  if(nrow(read_cache(.file_name = table_name)) == 0){
    #Create cache entry
    readRDS(cache_file) %>%
      bind_rows(tibble(
        "created"=Sys.time(),
        "updated"=as_date("1900-01-01"), #default date is used in initial POLIS query. It should be set to a value less than the min expected value. Once initial table has been pulled, the date of last pull will be saved here
        "file_type"="rds", #currently, hard-coded as RDS. Could revise to allow user to specify output file type (e.g. rds, csv, other)
        "file_name"= table_name,
        "latest_date"=as_date("1900-01-01"), #default date is used in initial POLIS query. It should be set to a value less than the min expected value. Once initial table has been pulled, the latest date in the table will be saved here
        "date_field" = field_name
      )) %>%
      write_rds(cache_file)
  }
}


#Read cache_dir and return table-name, last-update, and latest-date to the working environment
read_table_in_cache_dir <- function(table_name){
  folder <- load_specs()$polis_data_folder
  cache_dir <- file.path(folder, "cache_dir")
  cache_file <- file.path(cache_dir, "cache.rds")
  #if a row with the table_name exists within cache, then pull the values from that row
  if(nrow(read_cache(.file_name = table_name)) != 0){
    updated <- (readRDS(cache_file) %>%
                  filter(file_name == table_name))$updated %>%
      as.Date()
    latest_date <- (readRDS(cache_file) %>%
                      filter(file_name == table_name))$latest_date %>%
      as.Date()
    table_name <- (readRDS(cache_file) %>%
                     filter(file_name == table_name))$file_name
    field_name <- (readRDS(cache_file) %>%
                     filter(file_name == table_name))$date_field
  }
  return(list = list("updated" = updated, "latest_date" = latest_date, "field_name" = field_name))
}
#' Read cache and return information
#' @param .file_name A string describing the file name for which you want information
#' @return tibble row which can be atomically accessed
read_cache <- function(.file_name = table_name,
                       cache_file = file.path(load_specs()$polis_data_folder, 'cache_dir','cache.rds')){
  readRDS(cache_file) %>%
    filter(file_name == .file_name)
}

#Get user input for which table to pull, if not specified:
prompt_user_input <- function(){
  defaults <- load_defaults()
  table_list <- c((defaults %>%
                     filter(!grepl("Indicator", table_name) & !grepl("RefData", table_name)))$table_name_descriptive, "Indicator", "Reference Data")
  indicator_list <- (defaults %>%
                       filter(grepl("Indicator", table_name) & !grepl("RefData", table_name)) %>%
                       mutate(table_name_descriptive = str_remove(table_name_descriptive, "Indicator: ")))$table_name_descriptive
  reference_list <- (defaults %>%
                       filter(grepl("RefData", table_name)) %>%
                       mutate(table_name_descriptive = str_remove(table_name_descriptive, "Reference Data: ")))$table_name_descriptive
  table_input <- table_list[utils::menu(table_list, title="Select a POLIS table to download:")]
  if(table_input == "Indicator"){
    indicator_input <- paste0("Indicator: ", indicator_list[utils::menu(indicator_list, title="Select an Indicator to download:")])
    table_defaults <- defaults %>%
      filter(indicator_input == table_name_descriptive)
  }
  if(table_input == "Reference Data"){
    reference_input <- paste0("Reference Data: ", reference_list[utils::menu(reference_list, title="Select a Reference Table to download:")])
    table_defaults <- defaults %>%
      filter(reference_input == table_name_descriptive)
  }
  if(!(table_input %in% c("Indicator", "Reference Data"))){
    table_defaults <- defaults %>%
      filter(table_input == table_name_descriptive)
  }
  return(table_defaults)
}

load_defaults <- function(){
  defaults <- as.data.frame(bind_rows(
    c(table_name_descriptive = "Activity", table_name = "Activity", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Case", table_name = "Case", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Environmental Sample", table_name = "EnvSample", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Geography", table_name = "Geography", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP 0 dose percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP 1 to 2 dose percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_1_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP 3+ doses percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: AFP cases", table_name = "IndicatorValue('AFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Circulating VDPV case count (all Serotypes)", table_name = "IndicatorValue('cVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Circulating VDPV cace count (all serotypes) - Reporting", table_name = "IndicatorValue('cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Combined Surveillance Indicators category", table_name = "IndicatorValue('SURVINDCAT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Environmental sample circulating VDPV", table_name = "IndicatorValue('ENV_CVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Environmental samples count", table_name = "IndicatorValue('ENV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Environmental Wild samples", table_name = "IndicatorValue('ENV_WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Non polio AFP cases (under 15Y)", table_name = "IndicatorValue('NPAFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending excluded)", table_name = "IndicatorValue('NPAFP_RATE_NOPENDING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending included)", table_name = "IndicatorValue('NPAFP_RATE')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: NPAFP 0-2 dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_0_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: NPAFP 0 dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: NPAFP 3+ dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of 60-day follow-up cases with inadequate stool", table_name = "IndicatorValue('FUP_INSA_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of cases not classified", table_name = "IndicatorValue('UNCLASS_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of cases w/ adeq stools specimens (condition+timeliness)", table_name = "IndicatorValue('NPAFP_SA_WithStoolCond')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Percent of cases with two specimens within 14 days of onset", table_name = "IndicatorValue('NPAFP_SA')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Reported circulating VDPV environmental samples count (all serotypes)", table_name = "IndicatorValue('ENV_cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Reported Wild environmental samples count", table_name = "IndicatorValue('ENV_WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA bOPV campaigns", table_name = "IndicatorValue('SIA_BOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA mOPV campaigns", table_name = "IndicatorValue('SIA_MOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA planned since last case", table_name = "IndicatorValue('SIA_LASTCASE_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: SIA tOPV campaigns", table_name = "IndicatorValue('SIA_TOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Total SIA campaigns", table_name = "IndicatorValue('SIA_OPVTOT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Wild poliovirus case count", table_name = "IndicatorValue('WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Indicator: Wild poliovirus case count - Reporting", table_name = "IndicatorValue('WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Lab Specimen", table_name = "LabSpecimen", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "LQAS", table_name = "Lqas", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Population", table_name = "Population", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityCategories", table_name = "RefData('ActivityCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityDeletionReason", table_name = "RefData('ActivityDeletionReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityPhases", table_name = "RefData('ActivityPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityPriorities", table_name = "RefData('ActivityPriorities')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityStatuses", table_name = "RefData('ActivityStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ActivityTypes", table_name = "RefData('ActivityTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AdditionalInterventions", table_name = "RefData('AdditionalInterventions')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AgeGroups", table_name = "RefData('AgeGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AllocationStatuses", table_name = "RefData('AllocationStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: AwardTypes", table_name = "RefData('AwardTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CalendarTypes", table_name = "RefData('CalendarTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CaseClassification", table_name = "RefData('CaseClassification')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Categories", table_name = "RefData('Categories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CategoryValues", table_name = "RefData('CategoryValues')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CellCultureResultsLRarm", table_name = "RefData('CellCultureResultsLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CellCultureResultsRLRarm", table_name = "RefData('CellCultureResultsRLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Channels", table_name = "RefData('Channels')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContactIsolatedVdpv", table_name = "RefData('ContactIsolatedVdpv')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContactIsolatedWild", table_name = "RefData('ContactIsolatedWild')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContributionFlexibility", table_name = "RefData('ContributionFlexibility')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ContributionStatuses", table_name = "RefData('ContributionStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: CostCenters", table_name = "RefData('CostCenters')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Currencies", table_name = "RefData('Currencies')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Datasets", table_name = "RefData('Datasets')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DateTag", table_name = "RefData('DateTag')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DelayReason", table_name = "RefData('DelayReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DiagnosisFinal", table_name = "RefData('DiagnosisFinal')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: DocumentCategories", table_name = "RefData('DocumentCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: EmergenceGroups", table_name = "RefData('EmergenceGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FinalCellCultureResultAfp", table_name = "RefData('FinalCellCultureResultAfp')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FinalCellCultureResultLab", table_name = "RefData('FinalCellCultureResultLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FinalITDResults", table_name = "RefData('FinalITDResults')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FollowupFindings", table_name = "RefData('FollowupFindings')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: FundingPhases", table_name = "RefData('FundingPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Genders", table_name = "RefData('Genders')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Genotypes", table_name = "RefData('Genotypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Geolevels", table_name = "RefData('Geolevels')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ImportationEvents", table_name = "RefData('ImportationEvents')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed because API documentation states there is no data for this table
    c(table_name_descriptive = "Reference Data: IndependentMonitoringReasons", table_name = "RefData('IndependentMonitoringReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: IndependentMonitoringSources", table_name = "RefData('IndependentMonitoringSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: IndicatorCategories", table_name = "RefData('IndicatorCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: IntratypeLab", table_name = "RefData('IntratypeLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Intratypes", table_name = "RefData('Intratypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: LQASClassifications", table_name = "RefData('LQASClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: LQASDenominators", table_name = "RefData('LQASDenominators')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: LQASThresholds", table_name = "RefData('LQASThresholds')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Methodologies", table_name = "RefData('Methodologies')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Objectives", table_name = "RefData('Objectives')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Outbreaks", table_name = "RefData('Outbreaks')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ParalysisSite", table_name = "RefData('ParalysisSite')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: PartialInclusionReason", table_name = "RefData('PartialInclusionReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: PopulationSources", table_name = "RefData('PopulationSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: PosNeg", table_name = "RefData('PosNeg')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: PreviousNumberOfDoses", table_name = "RefData('PreviousNumberOfDoses')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ProgrammeAreas", table_name = "RefData('ProgrammeAreas')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ReasonMissed", table_name = "RefData('ReasonMissed')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ReasonVaccineRefused", table_name = "RefData('ReasonVaccineRefused')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Reports", table_name = "RefData('Reports')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ResultsElisa", table_name = "RefData('ResultsElisa')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ResultsPCR", table_name = "RefData('ResultsPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: ResultsRRTPCR", table_name = "RefData('ResultsRRTPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: RiskLevel", table_name = "RefData('RiskLevel')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SampleCondition", table_name = "RefData('SampleCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: Sectors", table_name = "RefData('Sectors')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SecurityStatus", table_name = "RefData('SecurityStatus')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SourceOfIsolate", table_name = "RefData('SourceOfIsolate')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SpecimenSampleType", table_name = "RefData('SpecimenSampleType')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SpecimenSource", table_name = "RefData('SpecimenSource')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: StoolCondition", table_name = "RefData('StoolCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: SurveillanceType", table_name = "RefData('SurveillanceType')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: UpdateType", table_name = "RefData('UpdateType')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: Vdpv2Clusters", table_name = "RefData('Vdpv2Clusters')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for Vdpv2Clusters reference data'
    c(table_name_descriptive = "Reference Data: VdpvClassifications", table_name = "RefData('VdpvClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: VdpvSources", table_name = "RefData('VdpvSources')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for VdpvSources reference data'
    c(table_name_descriptive = "Reference Data: VirusTypes", table_name = "RefData('VirusTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: VirusWildType", table_name = "RefData('VirusWildTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Data: YesNo", table_name = "RefData('YesNo')", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Sub Activity", table_name = "SubActivity", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Synonym", table_name = "Synonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Virus", table_name = "Virus", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Historized Geoplace Names", table_name = "HistorizedGeoplaceNames", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Historized Synonym", table_name = "HistorizedSynonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "IM LQAS Last Two Rounds", table_name = "ImLqasLastTwoRounds", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Enviro Sample Sites", table_name = "EnviroSampleSites", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "IM LQAS Since 3 Years Done Planned SIAs", table_name = "ImLqasSince3YearsDonePlannedSIAs", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "RefCluster", table_name = "RefCluster", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Reference Age Group Data", table_name = "RefAgeGroupData('AgeGroups')", field_name = "None", id_vars ="None", download_size = 1000),
    # c(table_name_descriptive = "PONS Store GR Environmental Samples", table_name = "PonsStoreGREnvironmentalSamples", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "PONS Store GR Cases", table_name = "PonsStoreGRCases", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Outbreak SIAs", table_name = "OutbreakSias", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "OprSopTracker", table_name = "OprSopTracker", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Independent Monitoring", table_name = "Im", field_name = "None", id_vars ="Id", download_size = 1000)
  ))
  return(defaults)
}