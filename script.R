library(tidyverse)
library(janitor)
library(odbc)
library(DBI)
library(scales)

################# Import data #################################################
# create the connection for your session
db <- DBI::dbConnect(odbc::odbc(), "coch_p2")

# import data from warehouse
ward_stays <- DBI::dbGetQuery(db, "
                              select
	ws.ENCNTR_SLICE_ID
	,ws.ENCNTR_ID
	,ws.LocalPatientIdentifier
	,ws.LocationName
	,ws.WardStartDateTime
	,ws.WardEndDateTime
	--,ws.DischargeDateTime
	,ws.LatestTreatmentFunctionMnemonic
	,lusd.Division
	,ws.WardSeqID
	,ws.WardSecCount
	,iif(convert(date, WardStartDateTime) < convert(date, WardEndDateTime), 1, 0) [OvernightStay]
	,iif(ws.ENCNTR_ID in (select distinct ENCNTR_ID from CernerStaging.[BI].[InpatientMDS]), 1, 0) [IMDSFlag]
	,absd.AgeOnAdmission
	,CASE WHEN PatientClassification = '1' THEN 'Ordinary'
		WHEN PatientClassification = '2' THEN 'Day case'
		WHEN PatientClassification = '3' THEN 'Regular day'
		WHEN PatientClassification = '4' THEN 'Regular night'
		WHEN PatientClassification = '5' THEN 'Delivery facilities only'
		ELSE PatientClassification
		END AS [PatientClassification]
	,CASE WHEN absd.ManagementIntentID = '1' THEN 'Overnight'
		WHEN absd.ManagementIntentID = '2' THEN 'Day case'
		WHEN absd.ManagementIntentID = '3' THEN 'Sequence (day and night)'
		WHEN absd.ManagementIntentID = '4' THEN 'Sequence (day)'
		WHEN absd.ManagementIntentID = '5' THEN 'Sequence (night only)'
		WHEN absd.ManagementIntentID = '8' THEN 'NA'
		WHEN absd.ManagementIntentID = '9' THEN 'Unknown'
		ELSE absd.ManagementIntentID
		END AS [IntendedManagementCode]
	,CASE WHEN absd.AdmissionMethodDDICT like '1%' THEN 'Elective'
			WHEN absd.AdmissionMethodDDICT like '2%' THEN 'Emergency'
			WHEN absd.AdmissionMethodDDICT like '3%' THEN 'Maternity'
			WHEN absd.AdmissionMethodDDICT like '8%' THEN 'Other'
			ELSE absd.AdmissionMethodDDICT
		END AS [AdmissionMethod]
	,absd.Sex
from CernerStaging.BI.IP_Ward_Stays ws
left join CernerStaging.[BI].[AbstractDataMini] absd on ws.ENCNTR_ID = absd.AbstractID
left join [InformationDB].[dbo].[LuSpecialtyDivision] lusd on lusd.Specialty = ws.LatestTreatmentFunctionMnemonic
--left join lumpi - address
--left join carehome lookups
where 1=1
	and WardEndDateTime > dateadd(week, -6, convert(date,getdate()))
	and (ws.DischargeDateTime > dateadd(week, -6, convert(date,getdate())) or ws.DischargeDateTime is null)
                              ") |>
  clean_names()

###############################################################################
get_mode_forcats <- function(x) {
  # Reorder levels by frequency, then get the name of the first level
  levels(fct_infreq(x))[1]
}


ward_summary <- ward_stays |>
  group_by(location_name) |>
  mutate(nel_flag = if_else(admission_method == "Emergency", 1, 0, missing = 0)) |>
  summarise(n = n(),
            imds_percent = sum(imds_flag)/n,
            overnight_percent = mean(overnight_stay),
            division = get_mode_forcats(division),
            specialty = get_mode_forcats(latest_treatment_function_mnemonic),
            nel_percent = percent(mean(nel_flag, na.rm = T)),
            admission_method = get_mode_forcats(admission_method)) |>
  mutate(ward_type = case_when(imds_percent < 0.1 ~ "Virtual",
                               overnight_percent > 0.2 ~ "Overnight",
                               TRUE ~ "Day Ward"
                               )
        
         ) |>
  ungroup() |>
  arrange(desc(n))

################ Export data ##################################################


DBI::dbDisconnect(db)
################ END ##########################################################