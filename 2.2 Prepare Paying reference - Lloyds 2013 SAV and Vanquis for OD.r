rm(list=ls())
.libPaths("H:/R/library")
library(data.table)
library(DAtoolpak)
library(dplyr)

setwd(dirname(rstudioapi::getSourceEditorContext()$path))

# cost path
path_name = "F:/ASA Portfolios/2. Valuation/2.1. Portfolio acquisition/GB/UK Data Queries/DB16 queries"

# Read the wide format reference output from the database.

ref = "../2 data prep/4. OD ref - Lloyds 2013 Sav and Vanquis.csv"
d <- fread(ref, na.strings=c("NULL",""), colClasses = c(Account_URN="character"), header = TRUE)

d[grepl("[\r\n]", Segment_Name), .(Segment_Name)]
d[grepl("[\r\n]", Group_1_Name), .(Group_1_Name)]

d[, lapply(.SD, sum), .SDcols = paste0("M-", 12:1), by =.(Nickname, Months_Since_Book_On)]

# shift pre-acq payments for some portfolios
  d[Nickname %in% c("SAV") | (Nickname == "Vanquis" & Months_Since_Book_On <70), 
    c(paste0("M-", 11:1)) := lapply(.SD, function(x) x), .SDcols = paste0("M-", 12:2)]
  
  d[Nickname %in% c("Vanquis") & Months_Since_Book_On >=70, 
    c(paste0("M-", 10:1)) := lapply(.SD, function(x) x), .SDcols = paste0("M-", 12:3)]

#############################################################################################################
############ IMPORTANT FILTER HERE ##########################################################################
#############################################################################################################

  port_bookon = d[, .N, by =.(Months_Since_Book_On, Portfolio_Name)][order(Months_Since_Book_On)]
  d[, .N, by =.(Nickname)]
  
  d[, lapply(.SD, sum), .SDcols = paste0("M", 1:100), by = .(Nickname, Months_Since_Book_On)]
  
  #### FILTER PAYING
  d = d[Sale_Segment_Type == "Paying"]
  
  ### filter out FV <100, otherwise some recovery rates will signigificantly twisted the forecast
  d = d[FV >= 100]
  
  
  d_long = melt(
    d, 
    id.vars = c("Account_URN", "FV"), 
    measure.vars = paste0("M", 1:63), 
    variable.name = "Month",
    value.name = "Payment")
  d_long[, Month := gsub("M", "", Month)]
  d_long[, RR := Payment / FV]
  d_long[order(RR)]
  RR_Outliers = d_long[RR > 1.5 | RR < -1.5][order(RR)] # 5 Unique accounts, FV in total 6272
  
  # exclude accounts with outlier RR to further avoid twisting the forecast
  d = d[!Account_URN %in% RR_Outliers$Account_URN]
 
#############################################################################################################
############ IMPORTANT FILTER END HERE ##########################################################################
#############################################################################################################

  d[, Product := NULL]
  setnames(d, "Product_Description", "Product")

  d[,.N, by =.(Nickname)][order(Nickname)]
  
  # Change the Group_1_Name of Lloyds Banking Group 2012 and 2013
  d[, Pool_Year := year(Book_On_Date)]
  d[, .N, by =.(Group_1_Name, Pool_Year)][order(Group_1_Name, Pool_Year)]
  
  d[, Port_Name := ifelse( Group_1_Name %in% c("The Lloyds Banking Group") & Pool_Year == 2012, "The Lloyds Banking Group 2012",
                           ifelse(Group_1_Name %in% c("The Lloyds Banking Group") & Pool_Year == 2013, "The Lloyds Banking Group 2013",
                                  ifelse(grepl("MBNA", Group_1_Name), "MBNA",
                                         Group_1_Name)))]



r <- copy(d)
  
  # !!! USING FULL PAYMENT FOR PAYING ACCOUNTS IN IMPLICIT LEGAL FORECAST
  r[, paste0("M", 1:100) := NULL]
  setnames(r, paste0("full_M",1:100), paste0("M", 1:100))

# Check count is in line with SQL.
  r[, .(CountCheck = .N, FVCheck = sum(FV)), by =.(Nickname)]
  r[, .(CountCheck = .N, FVCheck = sum(FV)), by =.(Months_Since_Book_On)][order(Months_Since_Book_On)]

# Cap FV.  Should this be done here or after KNN?
#r[FV > 20000, FV := 20000]


# View payment history of reference by Nickname.
NicknameReferencePaymentSummary <- r[, lapply(.SD, sum, na.rm = T), .SDcols = c(paste0("M-", 12:1), paste0("M", 1:70)), by= "Nickname"][order(Nickname)]

copy_table_f(NicknameReferencePaymentSummary)


# Creation of payment variables.
r[, `:=` (sumcoll1m = `M-1`,
          sumcoll3m = rowSums(r[, `M-1`:`M-3`]),
          sumcoll6m = rowSums(r[, `M-1`:`M-6`]),
          sumcoll12m = rowSums(r[, `M-1`:`M-12`]),
          payments1m = ifelse(r[, `M-1`] > 0, 1, 0),
          payments3m = rowSums(r[, `M-1`:`M-3`] > 0),
          payments6m = rowSums(r[, `M-1`:`M-6`] > 0),
          payments12m = rowSums(r[, `M-1`:`M-12`] > 0)
)
][, `:=` (paid1m = ifelse(sumcoll1m > 0, 1, 0),
          paid3m = ifelse(sumcoll3m > 0, 1, 0),
          paid6m = ifelse(sumcoll6m > 0, 1, 0),
          paid12m = ifelse(sumcoll12m > 0, 1, 0))]


# Set variables to keep in cleaned reference.
variables = c("Account_URN", "Nickname", "Segment", "Legal_Status", "Product", "Sale_Segment_Type",
              
              "Third_Party_Category",
              "FV","Last_Payment_Amount","Age","Claim_Age",
              
              "Months_To_Default","Months_To_ILD" ,"Months_Since_Last_Payment", "Months_Since_Book_On",
              "Months_Since_Default", "Is_Fairshare", "Is_Direct",
              "sumcoll1m", "sumcoll3m", "sumcoll6m", "payments1m", "payments3m",
              "payments6m", "paid1m", "paid3m", "paid6m",
              paste0("M-", 12:1),
              paste0("M",1:100)
)
sum(!variables %in% names(r))
variables[!variables %in% names(r)]


# Write prepared reference.

reference_prepared_paying_file_name = "reference/2.2 Paying reference -Lloyds 2013 SAV and Vanquis for OD - prepared.csv"

fwrite(r[,..variables], reference_prepared_paying_file_name)

######################################################################
## output paying reference for power BI
d[, dataset := "Reference"][, dataset_order := 2]
d[, Account_ID := NULL]

setnames(d,"Account_URN", "Account_ID")


d[, .N, by =.(Segment)]

Account_cols = c("dataset",	
                 "dataset_order",	
                 "Account_ID",	
                 "Nickname",
                 "Group_1_Name",
                 "Port_Name",
                 "Segment",	
                 
                 "Segment_Name",
                 #"Sale_Segment_Order",	
                 "Sale_Segment_Type",	
                 "Legal_Status",
                 #"IRR (Approved)",	
                 "Product",	
                 "FV",	
                 #"Forecast_Balance",	
                 "Interim cash",	
                 "Third_Party_Category",
                 "Last_Payment_Amount",	
                 "Age",	
                 "Claim_Age",	
                 #"Fairshare_Rate",	
                 "Months_To_Default",	
                 "Months_Since_Last_Payment",	
                 "Propensity",	
                 "Months_Since_Book_On",	
                 "Months_Since_Default",
                 "Months_To_ILD",
                 "Pre_Lit_Start_Date",
                 "LBC_Letter_Date",
                 "Claim_Date",
                 "Pool_Year")
Account_cols[!Account_cols %in% names(d)]
d[, `Interim cash` := 0.0]
ref_accounts = d[, ..Account_cols]

ref_payments_amic = d[,c("Account_ID", grep("^M-[[:digit:]]|^M[[:digit:]]", names(d), value = T)), with = F]

# legal payments, just the full payment minus amicable payments
ref_payments_legal = cbind(
  d[,.(Account_ID)], 
  d[,c(grep("^full_M[[:digit:]]", names(d), value = T)), with = F]-
    d[,c(grep("^M[[:digit:]]", names(d), value = T)), with = F])





ref_payments_amic_long = melt(
  ref_payments_amic, 
  id.vars = c("Account_ID"), 
  variable.name = "Month", 
  value.name = "Payment")

ref_payments_legal_long = melt(
  ref_payments_legal, 
  id.vars = c("Account_ID"), 
  variable.name = "Month", 
  value.name = "Payment")

ref_payments_amic_long[, Month := as.integer(gsub("M", "", as.character(Month)))]
ref_payments_legal_long[, Month := as.integer(gsub("full_M|M", "", as.character(Month)))]

# import cost data in long format
ref_cost_long = fread(paste0(path_name, "/3.b Amic and Legal Costs.csv"), na.strings = c("NULL", ""), colClasses = c(Account_URN = "character"), header = TRUE)

# FILTER COST FOR RUNNING FASTER
  ref_cost_long = ref_cost_long[Account_URN %in% d$Account_ID]

# we only keep cost from month1. Because in collection data we don't keep month 0
ref_cost_long = ref_cost_long[Month >0]
ref_cost_Amic_long = ref_cost_long[, .(Account_ID = Account_URN, Month, Cost = Costs_Level_2 - Legal_Cost)]
ref_cost_Legal_long = ref_cost_long[, .(Account_ID = Account_URN, Month, Cost = Legal_Cost)]


# merge cost into payments
# Amic
ref_payment_cost_amic_long = merge(
  ref_payments_amic_long,
  ref_cost_Amic_long,
  by = c("Account_ID", "Month"),
  all.x = T
)

ref_payment_cost_legal_long = merge(
  ref_payments_legal_long,
  ref_cost_Legal_long,
  by = c("Account_ID", "Month"),
  all.x = T
)

# make probability
ref_payment_cost_amic_long[, Probability := ifelse(Payment >0, 1, 0)]
ref_payment_cost_legal_long[, Probability := ifelse(Payment >0, 1, 0)]

# cash type
ref_payment_cost_amic_long[, Cash_Type:= "Amic"]
ref_payment_cost_legal_long[, Cash_Type:= "Legal"]

# combine amic and legal cash and cost
ref_long = rbind(ref_payment_cost_amic_long, ref_payment_cost_legal_long, fill = TRUE)

ref_long[is.na(Cost), Cost:= 0]
ref_long[is.na(Probability), Probability :=0]

ref_long = ref_long[Payment !=0 | Cost !=0]


#save(ref_accounts, file = "power_bi/2.3 Paying ref Accounts_Belfast I MBNA Lloyds.rdata")
fwrite(ref_accounts, file = "power_bi/2.3 Paying reference Accounts - Lloyds 2013 SAV and Vanquis.csv")
#save(ref_payments_long, file = "power_bi/2.4 Paying ref Payments_Belfast I MBNA and Lloyds.rdata")
fwrite(ref_long, file = "power_bi/2.4 Paying reference Payments - long - Lloyds 2013 SAV and Vanquis.csv")







