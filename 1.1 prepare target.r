rm(list=ls())
.libPaths("H:/R/library")
library(data.table)
library(openxlsx)
library(DAtoolpak)
library(dplyr)
setwd(dirname(rstudioapi::getSourceEditorContext()$path))
getwd()
#source("./0. parameters.r")
#setwd(working_directory)

# cost path
path_name = "F:/ASA Portfolios/2. Valuation/2.1. Portfolio acquisition/GB/UK Data Queries/DB16 queries"


# import target
target_input_file_name = "target/1. LBG FF - after scrubbing LBG FF pre-payments.csv"
d = fread(target_input_file_name,na.strings=c("NULL",""), 
          colClasses = c(Account_URN="character"), 
          header = TRUE)

# import IVA exclusions
IVA = fread("target/Core - Exclusions.csv", colClasses = c(debt_ref = "character"))


d[, .N, by = .(Segment)]

d[, .N, by =.(Product_Description)]

d[, Product := Product_Description]
d[, Sale_Segment_Order := as.integer(
                          case_when(Segment == "Card-Paying high" ~ "1",
                                    Segment=="Card-Paying low" ~ "2",
                                    Segment=="Loan-Paying high" ~ "3",
                                    Segment=="Loan-Paying low" ~ "4",
                                    Segment=="OD-Paying high" ~ "5",
                                    Segment=="OD-Paying low" ~ "6",
                                    TRUE ~ as.character(Segment))
                          )]

# product dummies
d[Product == "Credit Card", Is_Card := 1][is.na(Is_Card), Is_Card := 0]
d[Product == "Overdraft Account", Is_OD := 1][is.na(Is_OD), Is_OD := 0]

d[, .N, by =.(Product, Is_Card, Is_OD)][order(Product, Is_Card, Is_OD)]

#d[, sum(`Interim cash`)]

d[,.N, by =.(Segment, Sale_Segment_Order)]

target = copy(d)

target[, .N, by =.(Nickname)]

# Valuation_Balance is at 210 days, not the sale as Sale_Balance, which is 120 days
target[FV <=0]
#summary(r)

target[, `:=` (
  sumcoll1m = `M-1`,
  sumcoll3m = rowSums(target[, `M-1`:`M-3`]),
  sumcoll6m = rowSums(target[, `M-1`:`M-6`]),
  sumcoll12m = rowSums(target[, `M-1`:`M-12`]),
  payments1m = ifelse(target[, `M-1`] > 0, 1, 0),
  payments3m = rowSums(target[, `M-1`:`M-3`] > 0),
  payments6m = rowSums(target[, `M-1`:`M-6`] > 0),
  payments12m = rowSums(target[, `M-1`:`M-12`] > 0)
)
]


target[, `:=` (paid1m = ifelse(sumcoll1m > 0, 1, 0),
          paid3m = ifelse(sumcoll3m > 0, 1, 0),
          paid6m = ifelse(sumcoll6m > 0, 1, 0),
          paid12m = ifelse(sumcoll12m > 0, 1, 0))]



variables = c(#"IN_SALE",
              "Account_URN",
              "Nickname", 
              "Group_1_Name",
              "Segment", 
              "Segment1",
              "Segment1_Order",
              #"Seller_Segment",
              "Legal_Status", 
              "Product", 
              "Product_Order",
              "Sale_Segment_Type",
              "Third_Party_Category",
              "FV",
              #"Sale_Balance",
              "Last_Payment_Amount",
              "Age",
              "Claim_Age",
              "Months_To_Default",
              "Months_Since_Last_Payment", 
              "Months_Since_Book_On",
              "Months_Since_Default", 
              #"Months_On_Plan",
              "Is_Fairshare", 
              "Is_Direct",
              "sumcoll1m", 
              "sumcoll3m", 
              "sumcoll6m", 
              "payments1m", 
              "payments3m",
              "payments6m", 
              "paid1m", 
              "paid3m", 
              "paid6m",
              "Propensity",
              "Contact_Score_L1",
 
              "Sale_Segment_Order",
              "Fairshare_Rate",
              "Is_Card",
              "Is_OD",
          
              
              
              paste0("M-", 12:1)

)
sum(!variables %in% names(target))

variables[!variables %in% names(target)] # missing Months_Since_Book_On


target[, Months_Since_Book_On := 180]

target_model = target[, ..variables]


fwrite(target_model, file = "target/LBG FF target batches - processed.csv")

################################################################################################
################## OUTPUT FOR POWER BI  ########################################################
################################################################################################

# merge linked accounts individual ID

d[, .(Account_ID, Account_URN)]
d[, Account_ID := NULL]
setnames(d, "Account_URN", "Account_ID")

d[, Months_On_Book_Orig := Months_Since_Book_On]

output_cols = c(# "IN_SALE",
                "dataset",
                "dataset_order",
                "Group_1_Name",

                "Account_ID",
                "Nickname",
                "Segment",
                "Segment1",
                "Segment1_Order",
                "Legal_Status",
                "IRR (Approved)",
                "Product",
                "Product_Order",
                "Sale_Segment_Type",
                "Third_Party_Category",
                "FV",
                # "Valuation_Balance",
                
                "Interim cash",
                "Last_Payment_Amount",
                "Age",
                "Claim_Age",
                "Fairshare_Rate",
                "Months_To_Default",
                "Months_To_ILD",
                "Months_Since_Last_Payment",
                "Months_Since_Book_On",
                "Months_On_Book_Orig",
                "Months_Since_Default",
                # "Months_On_Plan",
                
                "Sale_Segment_Order",
                
                # "Is_Email",
                # "IS_PHONE", 
                
                
                "Propensity",
                "Contact_Score_L1"#,
               

              
                
                # linked existing accounts, individual ID columns
                # ,"Individual_ID"
                # ,"linked_accounts"
                # ,"N_Existing_Accs"
                )

output_cols[!output_cols %in% names(d)]


#### prepare columns
#filter paying accounts
#target = target[Sale_Segment_Type == "Paying"]
d[, dataset := "Target"][, dataset_order := 1]

d[, Port_Name := "Target"]

d[, Nickname_Order := 1]
d[, .N, by =.(Segment)]

d[, `IRR (Approved)` := 0.10]

d[, `Interim cash` := 0]


d[, .N, by =.(Segment, `IRR (Approved)`)]
d[, Months_Since_Book_On := 180]
# d[, Gone_Away := NULL]
# d[, Gone_Away := Gone_Away_Start]


# Are all columns in target?
output_cols[!output_cols %in% names(d)] 

Target_PreAcq_pmts = d[, c("Account_ID", grep("^M-[[:digit:]]", names(d), value = T)), with = F]

Target_PreAcq_pmts_long = melt(Target_PreAcq_pmts, id.vars = "Account_ID", value.name = "Payment", variable.name = "Month")
Target_PreAcq_pmts_long[, Month := as.integer(sub("M", "", as.character(Month)))]
Target_PreAcq_pmts_long[Month ==0, sum(Payment)]
Target_PreAcq_pmts_long = Target_PreAcq_pmts_long[Payment != 0]
Target_PreAcq_pmts_long[, Probability :=ifelse(Payment >0, 1, 0)]

Target_PreAcq_pmts_long[, Cash_Type := "Amic"]
# output
target_accounts = d[, ..output_cols]


save(target_accounts, file = "power_bi/1.1 target_accounts.rdata")
fwrite(target_accounts, file = "power_bi/1.1 target_accounts.csv")
# save(Target_PreAcq_pmts_long, file = "power_bi/1.2 Target_PreAcq_pmts_long.rdata")
fwrite(Target_PreAcq_pmts_long, file = "power_bi/pbi_files/2. Target_Pre_Acq_pmts_long.csv")

# output for used reference
# target_used = d[,.(Account_URN = Account_ID, times_used = 1, Is_used = 1, Model = Segment)]
# fwrite(target_used, "power_bi/5 reference used/5.3 used target.csv")
