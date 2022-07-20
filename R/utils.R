#' Load authorizations and local config
#' @param file A string
#' @return auth/config object
load_specs <- function(folder = Sys.getenv("polis_data_folder")){
  specs <- read_yaml(file.path(folder,'cache_dir','specs.yaml'))
  return(specs)
}

#' Load query parameters
#' @param file A string
#' @return auth/config object
load_query_parameters <- function(folder = Sys.getenv("polis_data_folder")){
  query_parameters <- read_yaml(file.path(folder,'cache_dir','query_parameters.yaml'))
  return(query_parameters)
}

load_defaults <- function(){
  defaults <- as.data.frame(bind_rows(
    c(table_name_descriptive = "Activity", table_name = "Activity", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Case", table_name = "Case", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Environmental Sample", table_name = "EnvSample", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Geography", table_name = "Geography", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP 0 dose percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP 1 to 2 dose percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_1_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP 3+ doses percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP cases", table_name = "IndicatorValue('AFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Circulating VDPV case count (all Serotypes)", table_name = "IndicatorValue('cVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Circulating VDPV cace count (all serotypes) - Reporting", table_name = "IndicatorValue('cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Combined Surveillance Indicators category", table_name = "IndicatorValue('SURVINDCAT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Environmental sample circulating VDPV", table_name = "IndicatorValue('ENV_CVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Environmental samples count", table_name = "IndicatorValue('ENV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Environmental Wild samples", table_name = "IndicatorValue('ENV_WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Non polio AFP cases (under 15Y)", table_name = "IndicatorValue('NPAFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending excluded)", table_name = "IndicatorValue('NPAFP_RATE_NOPENDING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending included)", table_name = "IndicatorValue('NPAFP_RATE')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: NPAFP 0-2 dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_0_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: NPAFP 0 dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: NPAFP 3+ dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of 60-day follow-up cases with inadequate stool", table_name = "IndicatorValue('FUP_INSA_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of cases not classified", table_name = "IndicatorValue('UNCLASS_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of cases w/ adeq stools specimens (condition+timeliness)", table_name = "IndicatorValue('NPAFP_SA_WithStoolCond')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of cases with two specimens within 14 days of onset", table_name = "IndicatorValue('NPAFP_SA')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Reported circulating VDPV environmental samples count (all serotypes)", table_name = "IndicatorValue('ENV_cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Reported Wild environmental samples count", table_name = "IndicatorValue('ENV_WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA bOPV campaigns", table_name = "IndicatorValue('SIA_BOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA mOPV campaigns", table_name = "IndicatorValue('SIA_MOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA planned since last case", table_name = "IndicatorValue('SIA_LASTCASE_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA tOPV campaigns", table_name = "IndicatorValue('SIA_TOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Total SIA campaigns", table_name = "IndicatorValue('SIA_OPVTOT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Wild poliovirus case count", table_name = "IndicatorValue('WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Wild poliovirus case count - Reporting", table_name = "IndicatorValue('WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Lab Specimen", table_name = "LabSpecimen", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "LQAS", table_name = "Lqas", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Population", table_name = "Population", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityCategories", table_name = "RefData('ActivityCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityDeletionReason", table_name = "RefData('ActivityDeletionReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityPhases", table_name = "RefData('ActivityPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityPriorities", table_name = "RefData('ActivityPriorities')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityStatuses", table_name = "RefData('ActivityStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityTypes", table_name = "RefData('ActivityTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AdditionalInterventions", table_name = "RefData('AdditionalInterventions')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AgeGroups", table_name = "RefData('AgeGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AllocationStatuses", table_name = "RefData('AllocationStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AwardTypes", table_name = "RefData('AwardTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CalendarTypes", table_name = "RefData('CalendarTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CaseClassification", table_name = "RefData('CaseClassification')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Categories", table_name = "RefData('Categories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CategoryValues", table_name = "RefData('CategoryValues')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CellCultureResultsLRarm", table_name = "RefData('CellCultureResultsLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CellCultureResultsRLRarm", table_name = "RefData('CellCultureResultsRLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Channels", table_name = "RefData('Channels')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContactIsolatedVdpv", table_name = "RefData('ContactIsolatedVdpv')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContactIsolatedWild", table_name = "RefData('ContactIsolatedWild')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContributionFlexibility", table_name = "RefData('ContributionFlexibility')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContributionStatuses", table_name = "RefData('ContributionStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CostCenters", table_name = "RefData('CostCenters')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Currencies", table_name = "RefData('Currencies')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Datasets", table_name = "RefData('Datasets')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DateTag", table_name = "RefData('DateTag')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DelayReason", table_name = "RefData('DelayReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DiagnosisFinal", table_name = "RefData('DiagnosisFinal')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DocumentCategories", table_name = "RefData('DocumentCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: EmergenceGroups", table_name = "RefData('EmergenceGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FinalCellCultureResultAfp", table_name = "RefData('FinalCellCultureResultAfp')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FinalCellCultureResultLab", table_name = "RefData('FinalCellCultureResultLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FinalITDResults", table_name = "RefData('FinalITDResults')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FollowupFindings", table_name = "RefData('FollowupFindings')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FundingPhases", table_name = "RefData('FundingPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Genders", table_name = "RefData('Genders')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Genotypes", table_name = "RefData('Genotypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Geolevels", table_name = "RefData('Geolevels')", field_name = "None", id_vars ="Id", download_size = 1000),
    # ## c(table_name_descriptive = "Reference Data: ImportationEvents", table_name = "RefData('ImportationEvents')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed because API documentation states there is no data for this table
    # c(table_name_descriptive = "Reference Data: IndependentMonitoringReasons", table_name = "RefData('IndependentMonitoringReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: IndependentMonitoringSources", table_name = "RefData('IndependentMonitoringSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: IndicatorCategories", table_name = "RefData('IndicatorCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: IntratypeLab", table_name = "RefData('IntratypeLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Intratypes", table_name = "RefData('Intratypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: LQASClassifications", table_name = "RefData('LQASClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: LQASDenominators", table_name = "RefData('LQASDenominators')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: LQASThresholds", table_name = "RefData('LQASThresholds')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Methodologies", table_name = "RefData('Methodologies')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Objectives", table_name = "RefData('Objectives')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Outbreaks", table_name = "RefData('Outbreaks')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ParalysisSite", table_name = "RefData('ParalysisSite')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PartialInclusionReason", table_name = "RefData('PartialInclusionReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PopulationSources", table_name = "RefData('PopulationSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PosNeg", table_name = "RefData('PosNeg')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: PreviousNumberOfDoses", table_name = "RefData('PreviousNumberOfDoses')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ProgrammeAreas", table_name = "RefData('ProgrammeAreas')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ReasonMissed", table_name = "RefData('ReasonMissed')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ReasonVaccineRefused", table_name = "RefData('ReasonVaccineRefused')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Reports", table_name = "RefData('Reports')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ResultsElisa", table_name = "RefData('ResultsElisa')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ResultsPCR", table_name = "RefData('ResultsPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ResultsRRTPCR", table_name = "RefData('ResultsRRTPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: RiskLevel", table_name = "RefData('RiskLevel')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SampleCondition", table_name = "RefData('SampleCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Sectors", table_name = "RefData('Sectors')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SecurityStatus", table_name = "RefData('SecurityStatus')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SourceOfIsolate", table_name = "RefData('SourceOfIsolate')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SpecimenSampleType", table_name = "RefData('SpecimenSampleType')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SpecimenSource", table_name = "RefData('SpecimenSource')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: StoolCondition", table_name = "RefData('StoolCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SurveillanceType", table_name = "RefData('SurveillanceType')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: UpdateType", table_name = "RefData('UpdateType')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: Vdpv2Clusters", table_name = "RefData('Vdpv2Clusters')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for Vdpv2Clusters reference data'
    # c(table_name_descriptive = "Reference Data: VdpvClassifications", table_name = "RefData('VdpvClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: VdpvSources", table_name = "RefData('VdpvSources')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for VdpvSources reference data'
    # c(table_name_descriptive = "Reference Data: VirusTypes", table_name = "RefData('VirusTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: VirusWildType", table_name = "RefData('VirusWildTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: YesNo", table_name = "RefData('YesNo')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Sub Activity", table_name = "SubActivity", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Synonym", table_name = "Synonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Virus", table_name = "Virus", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Historized Geoplace Names", table_name = "HistorizedGeoplaceNames", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Historized Synonym", table_name = "HistorizedSynonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "IM LQAS Last Two Rounds", table_name = "ImLqasLastTwoRounds", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Enviro Sample Sites", table_name = "EnviroSampleSites", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "IM LQAS Since 3 Years Done Planned SIAs", table_name = "ImLqasSince3YearsDonePlannedSIAs", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "RefCluster", table_name = "RefCluster", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Age Group Data", table_name = "RefAgeGroupData('AgeGroups')", field_name = "None", id_vars ="None", download_size = 1000),
    # c(table_name_descriptive = "PONS Store GR Environmental Samples", table_name = "PonsStoreGREnvironmentalSamples", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "PONS Store GR Cases", table_name = "PonsStoreGRCases", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Outbreak SIAs", table_name = "OutbreakSias", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "OprSopTracker", table_name = "OprSopTracker", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Independent Monitoring", table_name = "Im", field_name = "None", id_vars ="Id", download_size = 1000)
  ))
  return(defaults)
}

#Call an individual URL until it succeeds or reaches a call limit
#input: URL
#output: API response
call_url <- function(url,
                     error_action = "STOP"){
  status_code <- "x"
  i <- 1
  while(status_code != "200" & i < 10){
    response <- NULL
    response <- httr::GET(url, timeout(150))
    if(is.null(response) == FALSE){
      status_code <- as.character(response$status_code)
    }
    i <- i+1
    if(i == 10){
      if(error_action == "STOP"){
        stop("Query halted. Repeated API call failure.")
      }
      if(error_action == "RETURN NULL"){
        response <- NULL
      }
    }
    if(status_code != "200"){Sys.sleep(10)}
  }
  rm(status_code)
  return(response)
}
