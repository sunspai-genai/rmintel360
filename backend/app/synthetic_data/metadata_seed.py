from __future__ import annotations

from typing import Any

import duckdb


METADATA_TABLE_NAMES = [
    "metadata_access_policy",
    "metadata_synonym",
    "metadata_lineage",
    "metadata_join_path",
    "metadata_dimension",
    "metadata_metric",
    "metadata_business_term",
    "metadata_column",
    "metadata_table",
]


TABLE_METADATA = [
    (
        "dim_customer",
        "Commercial Customer",
        "Commercial Customers",
        "Dimension",
        "Commercial Banking",
        "One row per commercial banking customer.",
        "Reference",
        "Commercial Data Governance",
        True,
    ),
    (
        "dim_account",
        "Commercial Deposit Account",
        "Commercial Deposits",
        "Dimension",
        "Commercial Deposits",
        "One row per commercial deposit account.",
        "Reference",
        "Deposit Data Governance",
        True,
    ),
    (
        "dim_loan",
        "Commercial Loan",
        "Commercial Lending",
        "Dimension",
        "Commercial Lending",
        "One row per commercial loan or credit facility.",
        "Reference",
        "Loan Data Governance",
        True,
    ),
    (
        "dim_branch",
        "Branch And Market",
        "Organization",
        "Dimension",
        "Commercial Banking",
        "One row per branch or commercial banking office.",
        "Reference",
        "Branch Data Governance",
        True,
    ),
    (
        "dim_product",
        "Commercial Banking Product",
        "Products",
        "Dimension",
        "Commercial Banking",
        "One row per deposit or loan product.",
        "Reference",
        "Product Data Governance",
        True,
    ),
    (
        "dim_date",
        "Calendar Date",
        "Calendar",
        "Dimension",
        "Enterprise Calendar",
        "One row per calendar date.",
        "Reference",
        "Enterprise Data Governance",
        True,
    ),
    (
        "fact_deposit_balance_daily",
        "Daily Deposit Balance",
        "Commercial Deposits",
        "Fact",
        "Commercial Deposits",
        "One row per deposit account per business date.",
        "Daily",
        "Deposit Data Governance",
        True,
    ),
    (
        "fact_deposit_transaction",
        "Deposit Transaction",
        "Commercial Deposits",
        "Fact",
        "Commercial Deposits",
        "One row per commercial deposit transaction.",
        "Daily",
        "Deposit Operations",
        True,
    ),
    (
        "fact_loan_balance_monthly",
        "Monthly Loan Balance",
        "Commercial Lending",
        "Fact",
        "Commercial Lending",
        "One row per commercial loan per month-end snapshot.",
        "Monthly",
        "Loan Data Governance",
        True,
    ),
    (
        "fact_loan_payment",
        "Loan Payment",
        "Commercial Lending",
        "Fact",
        "Commercial Lending",
        "One row per commercial loan payment event.",
        "Monthly",
        "Loan Operations",
        True,
    ),
    (
        "fact_credit_risk_snapshot",
        "Credit Risk Snapshot",
        "Credit Risk",
        "Fact",
        "Credit Risk",
        "One row per customer per month-end risk snapshot.",
        "Monthly",
        "Credit Risk Governance",
        True,
    ),
    (
        "fact_relationship_profitability",
        "Relationship Profitability",
        "Relationship Profitability",
        "Fact",
        "Commercial Banking",
        "One row per customer per month-end profitability snapshot.",
        "Monthly",
        "Finance Data Governance",
        True,
    ),
]


COLUMN_METADATA = [
    ("dim_customer", "customer_id", "Customer Identifier", "INTEGER", "Synthetic surrogate key for a commercial customer.", "identifier", False, False, "101"),
    ("dim_customer", "customer_name", "Customer Name", "VARCHAR", "Commercial customer legal or relationship name.", "attribute", True, True, "Summit Manufacturing LLC"),
    ("dim_customer", "customer_segment", "Customer Segment", "VARCHAR", "Relationship segment assigned to the commercial customer.", "dimension", False, False, "Small Business, Middle Market, Corporate Banking"),
    ("dim_customer", "industry", "Industry", "VARCHAR", "Customer industry classification.", "dimension", False, False, "Manufacturing, Healthcare Services, Technology"),
    ("dim_customer", "naics_code", "NAICS Code", "VARCHAR", "North American Industry Classification System code or sector range.", "dimension", False, False, "31-33, 62, 54"),
    ("dim_customer", "revenue_band", "Revenue Band", "VARCHAR", "Estimated annual revenue range for the commercial customer.", "dimension", False, False, "$25MM-$50MM"),
    ("dim_customer", "risk_rating", "Customer Risk Rating", "INTEGER", "Internal commercial credit risk rating where higher values indicate weaker credit quality.", "dimension", False, False, "1, 2, 3, 4, 5, 6, 7"),
    ("dim_customer", "region", "Customer Region", "VARCHAR", "Relationship region assigned to the customer.", "dimension", False, False, "Southwest, West, Midwest"),
    ("dim_customer", "market", "Customer Market", "VARCHAR", "Relationship market assigned to the customer.", "dimension", False, False, "Phoenix, Dallas, Chicago"),
    ("dim_customer", "state", "Customer State", "VARCHAR", "Primary state for customer relationship management.", "dimension", False, False, "AZ, TX, CA"),
    ("dim_customer", "relationship_manager", "Relationship Manager", "VARCHAR", "Primary relationship manager assigned to the customer.", "dimension", False, False, "Avery Morgan"),
    ("dim_customer", "relationship_start_date", "Relationship Start Date", "DATE", "Date when the commercial banking relationship began.", "date", False, False, "2020-05-15"),
    ("dim_account", "account_id", "Account Identifier", "INTEGER", "Synthetic surrogate key for a commercial deposit account.", "identifier", False, False, "10001"),
    ("dim_account", "customer_id", "Customer Identifier", "INTEGER", "Customer key used to join deposit accounts to commercial customers.", "identifier", False, False, "101"),
    ("dim_account", "product_id", "Product Identifier", "INTEGER", "Product key used to join deposit accounts to product metadata.", "identifier", False, False, "101"),
    ("dim_account", "branch_id", "Branch Identifier", "INTEGER", "Branch key used to join deposit accounts to office and market metadata.", "identifier", False, False, "1"),
    ("dim_account", "account_number", "Account Number", "VARCHAR", "Synthetic commercial deposit account number.", "identifier", True, True, "DDA000001234"),
    ("dim_account", "account_type", "Deposit Account Type", "VARCHAR", "Deposit product account type.", "dimension", False, False, "DDA, Checking, Money Market"),
    ("dim_account", "open_date", "Account Open Date", "DATE", "Date the commercial deposit account was opened.", "date", False, False, "2021-09-30"),
    ("dim_account", "status", "Account Status", "VARCHAR", "Current lifecycle status for the deposit account.", "dimension", False, False, "Active, Dormant, Closed"),
    ("dim_account", "interest_bearing_flag", "Interest Bearing Flag", "BOOLEAN", "Indicates whether the deposit account earns interest.", "attribute", False, False, "true, false"),
    ("dim_loan", "loan_id", "Loan Identifier", "INTEGER", "Synthetic surrogate key for a commercial loan or credit facility.", "identifier", False, False, "20001"),
    ("dim_loan", "customer_id", "Customer Identifier", "INTEGER", "Customer key used to join loans to commercial customers.", "identifier", False, False, "101"),
    ("dim_loan", "product_id", "Product Identifier", "INTEGER", "Product key used to join loans to product metadata.", "identifier", False, False, "201"),
    ("dim_loan", "branch_id", "Branch Identifier", "INTEGER", "Branch key used to join loans to office and market metadata.", "identifier", False, False, "1"),
    ("dim_loan", "loan_number", "Loan Number", "VARCHAR", "Synthetic commercial loan account number.", "identifier", True, True, "LN000001234"),
    ("dim_loan", "loan_type", "Loan Type", "VARCHAR", "Commercial loan or facility type.", "dimension", False, False, "Revolver, Term Loan, CRE Loan"),
    ("dim_loan", "origination_date", "Origination Date", "DATE", "Date the commercial loan was originated.", "date", False, False, "2022-06-30"),
    ("dim_loan", "maturity_date", "Maturity Date", "DATE", "Contractual maturity date for the commercial loan.", "date", False, False, "2027-06-30"),
    ("dim_loan", "commitment_amount", "Commitment Amount", "DOUBLE", "Total committed credit amount for the loan or facility.", "measure", False, False, "2500000.00"),
    ("dim_loan", "rate_type", "Rate Type", "VARCHAR", "Indicates whether the commercial loan has a fixed or variable rate.", "dimension", False, False, "Fixed, Variable"),
    ("dim_loan", "interest_rate", "Interest Rate", "DOUBLE", "Current contractual interest rate for the loan.", "measure", False, False, "0.0725"),
    ("dim_loan", "collateral_type", "Collateral Type", "VARCHAR", "Primary collateral type supporting the loan.", "dimension", False, False, "Commercial Real Estate"),
    ("dim_loan", "status", "Loan Status", "VARCHAR", "Current lifecycle status for the loan.", "dimension", False, False, "Active, Matured, Charged Off"),
    ("dim_branch", "branch_id", "Branch Identifier", "INTEGER", "Synthetic surrogate key for a branch or commercial office.", "identifier", False, False, "1"),
    ("dim_branch", "branch_name", "Branch Name", "VARCHAR", "Commercial banking office or branch name.", "dimension", False, False, "Phoenix Commercial Banking Center"),
    ("dim_branch", "region", "Branch Region", "VARCHAR", "Geographic region for the branch or office.", "dimension", False, False, "Southwest, West, Southeast"),
    ("dim_branch", "market", "Branch Market", "VARCHAR", "Commercial banking market for the branch or office.", "dimension", False, False, "Phoenix, Denver, Dallas"),
    ("dim_branch", "state", "Branch State", "VARCHAR", "State for the branch or office.", "dimension", False, False, "AZ, CO, TX"),
    ("dim_branch", "city", "Branch City", "VARCHAR", "City for the branch or office.", "dimension", False, False, "Phoenix"),
    ("dim_product", "product_id", "Product Identifier", "INTEGER", "Synthetic surrogate key for a commercial banking product.", "identifier", False, False, "101"),
    ("dim_product", "product_code", "Product Code", "VARCHAR", "Short code for the commercial banking product.", "attribute", False, False, "DDA, RLOC, CRE"),
    ("dim_product", "product_name", "Product Name", "VARCHAR", "Business name of the commercial banking product.", "dimension", False, False, "Commercial Demand Deposit"),
    ("dim_product", "product_family", "Product Family", "VARCHAR", "High-level product family.", "dimension", False, False, "Deposits, Loans"),
    ("dim_product", "product_type", "Product Type", "VARCHAR", "Specific product type.", "dimension", False, False, "Money Market, Revolver"),
    ("dim_product", "product_segment", "Product Segment", "VARCHAR", "Reporting product segment used for portfolio analysis.", "dimension", False, False, "Operating Deposits, C&I Lending"),
    ("dim_product", "interest_bearing_flag", "Product Interest Bearing Flag", "BOOLEAN", "Indicates whether the product is interest bearing.", "attribute", False, False, "true, false"),
    ("dim_date", "calendar_date", "Calendar Date", "DATE", "Calendar date.", "date", False, False, "2026-03-31"),
    ("dim_date", "year", "Year", "INTEGER", "Calendar year.", "date_part", False, False, "2026"),
    ("dim_date", "quarter", "Quarter", "INTEGER", "Calendar quarter number.", "date_part", False, False, "1"),
    ("dim_date", "month", "Month", "INTEGER", "Calendar month number.", "date_part", False, False, "3"),
    ("dim_date", "month_name", "Month Name", "VARCHAR", "Calendar month name.", "date_part", False, False, "March"),
    ("dim_date", "year_month", "Year Month", "VARCHAR", "Calendar year-month in YYYY-MM format.", "date_part", False, False, "2026-03"),
    ("dim_date", "day_of_month", "Day Of Month", "INTEGER", "Day of month.", "date_part", False, False, "31"),
    ("dim_date", "day_of_week", "Day Of Week", "INTEGER", "ISO day of week.", "date_part", False, False, "2"),
    ("dim_date", "is_business_day", "Business Day Flag", "BOOLEAN", "Indicates whether the date is a weekday business day.", "date_part", False, False, "true"),
    ("dim_date", "is_month_end", "Month End Flag", "BOOLEAN", "Indicates whether the date is the last day of the month.", "date_part", False, False, "true"),
    ("dim_date", "fiscal_quarter", "Fiscal Quarter", "VARCHAR", "Fiscal quarter label.", "date_part", False, False, "FY2026 Q1"),
    ("fact_deposit_balance_daily", "as_of_date", "Balance As Of Date", "DATE", "Business date for the daily deposit balance snapshot.", "date", False, False, "2026-03-31"),
    ("fact_deposit_balance_daily", "account_id", "Account Identifier", "INTEGER", "Deposit account key used to join to dim_account.", "identifier", False, False, "10001"),
    ("fact_deposit_balance_daily", "ledger_balance", "Ledger Balance", "DOUBLE", "End-of-day posted commercial deposit balance.", "measure", False, False, "1250000.00"),
    ("fact_deposit_balance_daily", "collected_balance", "Collected Balance", "DOUBLE", "Deposit balance adjusted for float and funds availability.", "measure", False, False, "1195000.00"),
    ("fact_deposit_balance_daily", "available_balance", "Available Balance", "DOUBLE", "Collected deposit balance available for use after holds and restrictions.", "measure", False, False, "1130000.00"),
    ("fact_deposit_balance_daily", "average_balance_mtd", "Average Balance Month To Date", "DOUBLE", "Month-to-date average daily ledger balance for the account.", "measure", False, False, "1210000.00"),
    ("fact_deposit_balance_daily", "interest_rate", "Deposit Interest Rate", "DOUBLE", "Interest rate paid on the deposit account for interest-bearing products.", "measure", False, False, "0.015"),
    ("fact_deposit_balance_daily", "accrued_interest_mtd", "Accrued Interest Month To Date", "DOUBLE", "Month-to-date accrued interest expense for the deposit account.", "measure", False, False, "450.00"),
    ("fact_deposit_transaction", "transaction_id", "Transaction Identifier", "INTEGER", "Synthetic surrogate key for a deposit transaction.", "identifier", False, False, "30001"),
    ("fact_deposit_transaction", "transaction_date", "Transaction Date", "DATE", "Date the deposit transaction posted.", "date", False, False, "2026-03-15"),
    ("fact_deposit_transaction", "account_id", "Account Identifier", "INTEGER", "Deposit account key used to join to dim_account.", "identifier", False, False, "10001"),
    ("fact_deposit_transaction", "transaction_type", "Transaction Type", "VARCHAR", "Deposit transaction type.", "dimension", False, False, "ACH Credit, Wire Out"),
    ("fact_deposit_transaction", "channel", "Transaction Channel", "VARCHAR", "Channel used to originate or process the transaction.", "dimension", False, False, "ACH, Wire, Digital"),
    ("fact_deposit_transaction", "direction", "Transaction Direction", "VARCHAR", "Indicates whether the transaction is a credit or debit.", "dimension", False, False, "Credit, Debit"),
    ("fact_deposit_transaction", "amount", "Transaction Amount", "DOUBLE", "Posted deposit transaction amount.", "measure", False, False, "25000.00"),
    ("fact_deposit_transaction", "description", "Transaction Description", "VARCHAR", "Synthetic transaction description.", "attribute", False, False, "Incoming commercial wire"),
    ("fact_loan_balance_monthly", "as_of_month", "Loan Month End Date", "DATE", "Month-end date for the loan balance snapshot.", "date", False, False, "2026-03-31"),
    ("fact_loan_balance_monthly", "loan_id", "Loan Identifier", "INTEGER", "Loan key used to join to dim_loan.", "identifier", False, False, "20001"),
    ("fact_loan_balance_monthly", "outstanding_balance", "Outstanding Loan Balance", "DOUBLE", "Outstanding principal balance for the commercial loan.", "measure", False, False, "5000000.00"),
    ("fact_loan_balance_monthly", "commitment_amount", "Loan Commitment Amount", "DOUBLE", "Committed credit amount from the monthly loan snapshot.", "measure", False, False, "7500000.00"),
    ("fact_loan_balance_monthly", "available_credit", "Available Credit", "DOUBLE", "Unused committed credit available to the borrower.", "measure", False, False, "2500000.00"),
    ("fact_loan_balance_monthly", "utilization_rate", "Loan Utilization Rate", "DOUBLE", "Outstanding balance divided by commitment amount for the loan snapshot.", "measure", False, False, "0.667"),
    ("fact_loan_balance_monthly", "past_due_amount", "Past Due Amount", "DOUBLE", "Outstanding amount past due on the loan.", "measure", False, False, "50000.00"),
    ("fact_loan_balance_monthly", "delinquency_bucket", "Delinquency Bucket", "VARCHAR", "Days-past-due bucket for the loan.", "dimension", False, False, "Current, 30-59 DPD, 90+ DPD"),
    ("fact_loan_balance_monthly", "non_accrual_flag", "Non Accrual Flag", "BOOLEAN", "Indicates whether the loan is in non-accrual status.", "attribute", False, False, "true, false"),
    ("fact_loan_payment", "payment_id", "Payment Identifier", "INTEGER", "Synthetic surrogate key for a loan payment.", "identifier", False, False, "40001"),
    ("fact_loan_payment", "payment_date", "Payment Date", "DATE", "Date the loan payment was posted.", "date", False, False, "2026-03-15"),
    ("fact_loan_payment", "loan_id", "Loan Identifier", "INTEGER", "Loan key used to join to dim_loan.", "identifier", False, False, "20001"),
    ("fact_loan_payment", "principal_amount", "Principal Payment Amount", "DOUBLE", "Principal portion of the loan payment.", "measure", False, False, "15000.00"),
    ("fact_loan_payment", "interest_amount", "Interest Payment Amount", "DOUBLE", "Interest portion of the loan payment.", "measure", False, False, "21000.00"),
    ("fact_loan_payment", "fee_amount", "Loan Fee Amount", "DOUBLE", "Fee portion of the loan payment.", "measure", False, False, "125.00"),
    ("fact_credit_risk_snapshot", "as_of_month", "Risk Month End Date", "DATE", "Month-end date for the credit risk snapshot.", "date", False, False, "2026-03-31"),
    ("fact_credit_risk_snapshot", "customer_id", "Customer Identifier", "INTEGER", "Customer key used to join risk snapshots to dim_customer.", "identifier", False, False, "101"),
    ("fact_credit_risk_snapshot", "risk_rating", "Risk Rating", "INTEGER", "Month-end internal credit risk rating for the customer.", "dimension", False, False, "1, 2, 3, 4, 5, 6, 7"),
    ("fact_credit_risk_snapshot", "probability_of_default", "Probability Of Default", "DOUBLE", "Estimated one-year default probability for the customer.", "measure", False, False, "0.018"),
    ("fact_credit_risk_snapshot", "loss_given_default", "Loss Given Default", "DOUBLE", "Estimated percentage loss if the customer defaults.", "measure", False, False, "0.42"),
    ("fact_credit_risk_snapshot", "exposure_at_default", "Exposure At Default", "DOUBLE", "Estimated exposure if the customer defaults.", "measure", False, False, "3500000.00"),
    ("fact_credit_risk_snapshot", "watchlist_flag", "Watchlist Flag", "BOOLEAN", "Indicates whether the customer is on the commercial credit watchlist.", "attribute", False, False, "true, false"),
    ("fact_relationship_profitability", "as_of_month", "Profitability Month End Date", "DATE", "Month-end date for the relationship profitability snapshot.", "date", False, False, "2026-03-31"),
    ("fact_relationship_profitability", "customer_id", "Customer Identifier", "INTEGER", "Customer key used to join profitability snapshots to dim_customer.", "identifier", False, False, "101"),
    ("fact_relationship_profitability", "net_interest_income", "Net Interest Income", "DOUBLE", "Net interest income attributed to the customer relationship.", "measure", False, False, "25000.00"),
    ("fact_relationship_profitability", "fee_income", "Fee Income", "DOUBLE", "Fee income attributed to the customer relationship.", "measure", False, False, "5000.00"),
    ("fact_relationship_profitability", "allocated_expense", "Allocated Expense", "DOUBLE", "Allocated operating expense attributed to the relationship.", "measure", False, False, "7000.00"),
    ("fact_relationship_profitability", "provision_expense", "Provision Expense", "DOUBLE", "Expected credit provision expense attributed to the relationship.", "measure", False, False, "2500.00"),
    ("fact_relationship_profitability", "relationship_profit", "Relationship Profit", "DOUBLE", "Net relationship profit after income, fees, allocated expense, and provision expense.", "measure", False, False, "20500.00"),
]


BUSINESS_TERMS = [
    ("term.average_balance", "Average Balance", "metric", "A general business phrase that may refer to deposit ledger balance, deposit collected balance, or loan outstanding balance depending on context.", "Ambiguous; resolve to a certified metric before SQL generation.", None, None, True, "Commercial Data Governance", "Commercial Banking"),
    ("term.average_deposit_ledger_balance", "Average Deposit Ledger Balance", "metric", "Average daily posted ledger balance for commercial deposit accounts over a selected period.", "AVG(fact_deposit_balance_daily.ledger_balance)", "fact_deposit_balance_daily", "ledger_balance", True, "Deposit Data Governance", "Commercial Deposits"),
    ("term.average_deposit_collected_balance", "Average Deposit Collected Balance", "metric", "Average daily collected deposit balance adjusted for float and funds availability.", "AVG(fact_deposit_balance_daily.collected_balance)", "fact_deposit_balance_daily", "collected_balance", True, "Deposit Data Governance", "Commercial Deposits"),
    ("term.average_available_balance", "Average Available Balance", "metric", "Average daily deposit balance available for use after holds and restrictions.", "AVG(fact_deposit_balance_daily.available_balance)", "fact_deposit_balance_daily", "available_balance", True, "Deposit Data Governance", "Commercial Deposits"),
    ("term.ledger_balance", "Ledger Balance", "attribute", "End-of-day posted commercial deposit balance.", None, "fact_deposit_balance_daily", "ledger_balance", True, "Deposit Data Governance", "Commercial Deposits"),
    ("term.collected_balance", "Collected Balance", "attribute", "Deposit balance adjusted for float and funds availability.", None, "fact_deposit_balance_daily", "collected_balance", True, "Deposit Data Governance", "Commercial Deposits"),
    ("term.available_balance", "Available Balance", "attribute", "Collected deposit balance available after holds and restrictions.", None, "fact_deposit_balance_daily", "available_balance", True, "Deposit Data Governance", "Commercial Deposits"),
    ("term.customer_segment", "Customer Segment", "dimension", "Commercial banking relationship segment assigned to a customer.", None, "dim_customer", "customer_segment", True, "Commercial Data Governance", "Commercial Banking"),
    ("term.product_segment", "Product Segment", "dimension", "Product reporting segment used for deposit and loan portfolio analysis.", None, "dim_product", "product_segment", True, "Product Data Governance", "Commercial Banking"),
    ("term.loan_utilization", "Loan Utilization", "metric", "Commercial loan utilization calculated as outstanding balance divided by commitment amount.", "SUM(fact_loan_balance_monthly.outstanding_balance) / NULLIF(SUM(fact_loan_balance_monthly.commitment_amount), 0)", "fact_loan_balance_monthly", "utilization_rate", True, "Loan Data Governance", "Commercial Lending"),
    ("term.delinquency_rate", "Delinquency Rate", "metric", "Share of outstanding loan balance that is past due.", "SUM(CASE WHEN delinquency_bucket <> 'Current' THEN outstanding_balance ELSE 0 END) / NULLIF(SUM(outstanding_balance), 0)", "fact_loan_balance_monthly", "delinquency_bucket", True, "Credit Risk Governance", "Credit Risk"),
    ("term.risk_rating", "Risk Rating", "dimension", "Internal commercial credit risk rating where higher values indicate weaker credit quality.", None, "fact_credit_risk_snapshot", "risk_rating", True, "Credit Risk Governance", "Credit Risk"),
    ("term.relationship_profitability", "Relationship Profitability", "metric", "Profitability of a commercial customer relationship after income, fees, allocated expense, and provision expense.", "SUM(fact_relationship_profitability.relationship_profit)", "fact_relationship_profitability", "relationship_profit", True, "Finance Data Governance", "Commercial Banking"),
    ("term.lineage", "Data Lineage", "governance", "Description of source systems, transformations, and target governed assets.", None, None, None, True, "Enterprise Data Governance", "Enterprise Metadata"),
]


METRICS = [
    ("metric.average_deposit_ledger_balance", "Average Deposit Ledger Balance", "Average daily posted commercial deposit ledger balance.", "AVG(fdb.ledger_balance)", "AVG", "fact_deposit_balance_daily", "ledger_balance,as_of_date,account_id", "latest_complete_month", True, "Commercial Deposits"),
    ("metric.average_deposit_collected_balance", "Average Deposit Collected Balance", "Average daily collected commercial deposit balance adjusted for float.", "AVG(fdb.collected_balance)", "AVG", "fact_deposit_balance_daily", "collected_balance,as_of_date,account_id", "latest_complete_month", True, "Commercial Deposits"),
    ("metric.average_available_balance", "Average Available Balance", "Average daily available balance after holds and restrictions.", "AVG(fdb.available_balance)", "AVG", "fact_deposit_balance_daily", "available_balance,as_of_date,account_id", "latest_complete_month", True, "Commercial Deposits"),
    ("metric.total_commercial_deposits", "Total Commercial Deposits", "Total posted commercial deposit ledger balance.", "SUM(fdb.ledger_balance)", "SUM", "fact_deposit_balance_daily", "ledger_balance,as_of_date,account_id", "latest_complete_month", True, "Commercial Deposits"),
    ("metric.total_collected_deposits", "Total Collected Deposits", "Total collected commercial deposit balance adjusted for float.", "SUM(fdb.collected_balance)", "SUM", "fact_deposit_balance_daily", "collected_balance,as_of_date,account_id", "latest_complete_month", True, "Commercial Deposits"),
    ("metric.average_loan_outstanding_balance", "Average Loan Outstanding Balance", "Average outstanding commercial loan principal balance.", "AVG(flbm.outstanding_balance)", "AVG", "fact_loan_balance_monthly", "outstanding_balance,as_of_month,loan_id", "latest_complete_month", True, "Commercial Lending"),
    ("metric.total_loan_outstanding_balance", "Total Loan Outstanding Balance", "Total outstanding commercial loan principal balance.", "SUM(flbm.outstanding_balance)", "SUM", "fact_loan_balance_monthly", "outstanding_balance,as_of_month,loan_id", "latest_complete_month", True, "Commercial Lending"),
    ("metric.loan_commitment_amount", "Loan Commitment Amount", "Total committed credit amount for commercial loans and facilities.", "SUM(flbm.commitment_amount)", "SUM", "fact_loan_balance_monthly", "commitment_amount,as_of_month,loan_id", "latest_complete_month", True, "Commercial Lending"),
    ("metric.loan_utilization_rate", "Loan Utilization Rate", "Portfolio loan utilization calculated as ratio of outstanding balances to commitments.", "SUM(flbm.outstanding_balance) / NULLIF(SUM(flbm.commitment_amount), 0)", "RATIO_OF_SUMS", "fact_loan_balance_monthly", "outstanding_balance,commitment_amount,as_of_month,loan_id", "latest_complete_month", True, "Commercial Lending"),
    ("metric.loan_delinquency_rate", "Loan Delinquency Rate", "Percent of outstanding loan balance that is past due.", "SUM(CASE WHEN flbm.delinquency_bucket <> 'Current' THEN flbm.outstanding_balance ELSE 0 END) / NULLIF(SUM(flbm.outstanding_balance), 0)", "RATIO_OF_SUMS", "fact_loan_balance_monthly", "outstanding_balance,delinquency_bucket,as_of_month,loan_id", "latest_complete_month", True, "Credit Risk"),
    ("metric.past_due_amount", "Past Due Amount", "Total outstanding amount past due on commercial loans.", "SUM(flbm.past_due_amount)", "SUM", "fact_loan_balance_monthly", "past_due_amount,as_of_month,loan_id", "latest_complete_month", True, "Credit Risk"),
    ("metric.non_accrual_balance", "Non Accrual Balance", "Outstanding loan balance for loans in non-accrual status.", "SUM(CASE WHEN flbm.non_accrual_flag THEN flbm.outstanding_balance ELSE 0 END)", "SUM", "fact_loan_balance_monthly", "outstanding_balance,non_accrual_flag,as_of_month,loan_id", "latest_complete_month", True, "Credit Risk"),
    ("metric.watchlist_exposure", "Watchlist Exposure", "Exposure at default for customers on the commercial credit watchlist.", "SUM(CASE WHEN fcrs.watchlist_flag THEN fcrs.exposure_at_default ELSE 0 END)", "SUM", "fact_credit_risk_snapshot", "watchlist_flag,exposure_at_default,as_of_month,customer_id", "latest_complete_month", True, "Credit Risk"),
    ("metric.relationship_profit", "Relationship Profit", "Total commercial relationship profit after income, fees, allocated expense, and provision expense.", "SUM(frp.relationship_profit)", "SUM", "fact_relationship_profitability", "relationship_profit,as_of_month,customer_id", "latest_complete_month", True, "Commercial Banking"),
    ("metric.net_interest_income", "Net Interest Income", "Total net interest income attributed to commercial customer relationships.", "SUM(frp.net_interest_income)", "SUM", "fact_relationship_profitability", "net_interest_income,as_of_month,customer_id", "latest_complete_month", True, "Commercial Banking"),
    ("metric.fee_income", "Fee Income", "Total fee income attributed to commercial customer relationships.", "SUM(frp.fee_income)", "SUM", "fact_relationship_profitability", "fee_income,as_of_month,customer_id", "latest_complete_month", True, "Commercial Banking"),
]


DIMENSIONS = [
    ("dimension.customer_segment", "Customer Segment", "Commercial banking relationship segment assigned to the customer.", "dim_customer", "customer_segment", "Small Business, Business Banking, Middle Market, Corporate Banking, Commercial Real Estate", True, "Commercial Banking"),
    ("dimension.product_segment", "Product Segment", "Product reporting segment used for commercial banking portfolio analysis.", "dim_product", "product_segment", "Operating Deposits, Interest Bearing Deposits, C&I Lending, Commercial Real Estate", True, "Commercial Banking"),
    ("dimension.product_family", "Product Family", "High-level product family.", "dim_product", "product_family", "Deposits, Loans", True, "Commercial Banking"),
    ("dimension.product_type", "Product Type", "Specific deposit or loan product type.", "dim_product", "product_type", "DDA, Money Market, Revolver, CRE Loan", True, "Commercial Banking"),
    ("dimension.account_type", "Deposit Account Type", "Deposit account type for commercial deposit reporting.", "dim_account", "account_type", "DDA, Checking, Money Market, Savings, Sweep", True, "Commercial Deposits"),
    ("dimension.loan_type", "Loan Type", "Commercial loan or facility type.", "dim_loan", "loan_type", "Revolver, Term Loan, CRE Loan, Equipment Finance, SBA Loan, ABL", True, "Commercial Lending"),
    ("dimension.industry", "Industry", "Customer industry classification.", "dim_customer", "industry", "Manufacturing, Healthcare Services, Technology, Commercial Real Estate", True, "Commercial Banking"),
    ("dimension.naics_code", "NAICS Code", "NAICS code or sector range for customer industry classification.", "dim_customer", "naics_code", "31-33, 62, 54, 53", True, "Commercial Banking"),
    ("dimension.revenue_band", "Revenue Band", "Estimated annual revenue range for the commercial customer.", "dim_customer", "revenue_band", "$5MM-$10MM, $25MM-$50MM, >$1B", True, "Commercial Banking"),
    ("dimension.customer_region", "Customer Region", "Relationship region assigned to the customer.", "dim_customer", "region", "Southwest, West, Midwest, Southeast, Northeast", True, "Commercial Banking"),
    ("dimension.branch_region", "Branch Region", "Region of the branch or commercial banking office.", "dim_branch", "region", "Southwest, West, Midwest, Southeast, Northeast", True, "Commercial Banking"),
    ("dimension.market", "Market", "Commercial banking market.", "dim_branch", "market", "Phoenix, Dallas, Chicago, New York", True, "Commercial Banking"),
    ("dimension.state", "State", "State for customer or branch reporting.", "dim_branch", "state", "AZ, TX, CA, NY", True, "Commercial Banking"),
    ("dimension.relationship_manager", "Relationship Manager", "Primary relationship manager assigned to the customer.", "dim_customer", "relationship_manager", "Avery Morgan, Blake Chen, Cameron Patel", True, "Commercial Banking"),
    ("dimension.risk_rating", "Risk Rating", "Internal commercial credit risk rating.", "fact_credit_risk_snapshot", "risk_rating", "1, 2, 3, 4, 5, 6, 7", True, "Credit Risk"),
    ("dimension.delinquency_bucket", "Delinquency Bucket", "Days-past-due bucket for a loan.", "fact_loan_balance_monthly", "delinquency_bucket", "Current, 1-29 DPD, 30-59 DPD, 60-89 DPD, 90+ DPD", True, "Credit Risk"),
    ("dimension.rate_type", "Rate Type", "Fixed or variable commercial loan rate type.", "dim_loan", "rate_type", "Fixed, Variable", True, "Commercial Lending"),
    ("dimension.collateral_type", "Collateral Type", "Primary collateral type supporting the loan.", "dim_loan", "collateral_type", "Commercial Real Estate, Business Assets, Equipment", True, "Commercial Lending"),
    ("dimension.year_month", "Year Month", "Calendar year-month for trend reporting.", "dim_date", "year_month", "2024-01, 2025-12, 2026-03", True, "Enterprise Calendar"),
]


JOIN_PATHS = [
    ("join.deposit_balance_account", "fact_deposit_balance_daily", "account_id", "dim_account", "account_id", "many-to-one", "INNER", "Join daily deposit balances to deposit account attributes.", True),
    ("join.deposit_transaction_account", "fact_deposit_transaction", "account_id", "dim_account", "account_id", "many-to-one", "INNER", "Join deposit transactions to deposit account attributes.", True),
    ("join.account_customer", "dim_account", "customer_id", "dim_customer", "customer_id", "many-to-one", "INNER", "Join deposit accounts to commercial customer attributes.", True),
    ("join.account_product", "dim_account", "product_id", "dim_product", "product_id", "many-to-one", "INNER", "Join deposit accounts to product attributes.", True),
    ("join.account_branch", "dim_account", "branch_id", "dim_branch", "branch_id", "many-to-one", "INNER", "Join deposit accounts to branch and market attributes.", True),
    ("join.loan_balance_loan", "fact_loan_balance_monthly", "loan_id", "dim_loan", "loan_id", "many-to-one", "INNER", "Join monthly loan balances to loan attributes.", True),
    ("join.loan_payment_loan", "fact_loan_payment", "loan_id", "dim_loan", "loan_id", "many-to-one", "INNER", "Join loan payments to loan attributes.", True),
    ("join.loan_customer", "dim_loan", "customer_id", "dim_customer", "customer_id", "many-to-one", "INNER", "Join loans to commercial customer attributes.", True),
    ("join.loan_product", "dim_loan", "product_id", "dim_product", "product_id", "many-to-one", "INNER", "Join loans to product attributes.", True),
    ("join.loan_branch", "dim_loan", "branch_id", "dim_branch", "branch_id", "many-to-one", "INNER", "Join loans to branch and market attributes.", True),
    ("join.risk_customer", "fact_credit_risk_snapshot", "customer_id", "dim_customer", "customer_id", "many-to-one", "INNER", "Join credit risk snapshots to commercial customer attributes.", True),
    ("join.profitability_customer", "fact_relationship_profitability", "customer_id", "dim_customer", "customer_id", "many-to-one", "INNER", "Join relationship profitability to commercial customer attributes.", True),
    ("join.deposit_balance_date", "fact_deposit_balance_daily", "as_of_date", "dim_date", "calendar_date", "many-to-one", "INNER", "Join daily deposit balances to calendar attributes.", True),
    ("join.deposit_transaction_date", "fact_deposit_transaction", "transaction_date", "dim_date", "calendar_date", "many-to-one", "INNER", "Join deposit transactions to calendar attributes.", True),
    ("join.loan_balance_date", "fact_loan_balance_monthly", "as_of_month", "dim_date", "calendar_date", "many-to-one", "INNER", "Join monthly loan balances to calendar attributes.", True),
    ("join.loan_payment_date", "fact_loan_payment", "payment_date", "dim_date", "calendar_date", "many-to-one", "INNER", "Join loan payments to calendar attributes.", True),
    ("join.risk_date", "fact_credit_risk_snapshot", "as_of_month", "dim_date", "calendar_date", "many-to-one", "INNER", "Join credit risk snapshots to calendar attributes.", True),
    ("join.profitability_date", "fact_relationship_profitability", "as_of_month", "dim_date", "calendar_date", "many-to-one", "INNER", "Join relationship profitability snapshots to calendar attributes.", True),
]


LINEAGE = [
    ("lineage.deposit_ledger_balance", "Core Deposit Platform", "Daily Account Balance Snapshot", "fact_deposit_balance_daily", "ledger_balance", "End-of-day posted balances are loaded after nightly core deposit processing.", "Daily", "Deposit Data Governance"),
    ("lineage.deposit_collected_balance", "Core Deposit Platform", "Funds Availability Engine", "fact_deposit_balance_daily", "collected_balance", "Ledger balance is adjusted for float, clearing status, and funds availability.", "Daily", "Deposit Data Governance"),
    ("lineage.deposit_available_balance", "Core Deposit Platform", "Funds Availability Engine", "fact_deposit_balance_daily", "available_balance", "Collected balance is reduced by holds and restrictions to produce available balance.", "Daily", "Deposit Data Governance"),
    ("lineage.deposit_transactions", "Deposit Operations Platform", "Posted Deposit Transactions", "fact_deposit_transaction", "amount", "Commercial deposit transactions are standardized into credit and debit movements.", "Daily", "Deposit Operations"),
    ("lineage.loan_outstanding_balance", "Commercial Loan Servicing", "Month End Loan Trial Balance", "fact_loan_balance_monthly", "outstanding_balance", "Outstanding principal balances are captured from loan servicing at month end.", "Monthly", "Loan Data Governance"),
    ("lineage.loan_commitment_amount", "Commercial Loan Servicing", "Credit Facility Master", "fact_loan_balance_monthly", "commitment_amount", "Committed credit amounts are sourced from facility master records.", "Monthly", "Loan Data Governance"),
    ("lineage.loan_delinquency", "Commercial Loan Servicing", "Past Due Aging", "fact_loan_balance_monthly", "delinquency_bucket", "Days-past-due values are bucketed into governed delinquency categories.", "Monthly", "Credit Risk Governance"),
    ("lineage.customer_segment", "Customer Master", "Relationship Management Segmentation", "dim_customer", "customer_segment", "Relationship segment is assigned by commercial banking relationship management.", "Reference", "Commercial Data Governance"),
    ("lineage.product_segment", "Product Master", "Product Reporting Hierarchy", "dim_product", "product_segment", "Product hierarchy is curated by product data governance.", "Reference", "Product Data Governance"),
    ("lineage.risk_rating", "Credit Risk Platform", "Customer Risk Rating Model", "fact_credit_risk_snapshot", "risk_rating", "Risk ratings are sourced from the credit risk platform at month end.", "Monthly", "Credit Risk Governance"),
    ("lineage.probability_of_default", "Credit Risk Platform", "PD Model Output", "fact_credit_risk_snapshot", "probability_of_default", "Probability of default is produced by the commercial credit risk model.", "Monthly", "Credit Risk Governance"),
    ("lineage.relationship_profit", "Finance Allocation Engine", "Relationship Profitability Mart", "fact_relationship_profitability", "relationship_profit", "Relationship profit is calculated from net interest income, fee income, allocated expense, and provision expense.", "Monthly", "Finance Data Governance"),
]


SYNONYMS = [
    ("syn.average_balance.deposit_ledger", "average balance", "metric", "metric.average_deposit_ledger_balance", 0.74, "Common banking shorthand; ambiguous without deposit or loan context."),
    ("syn.avg_balance.deposit_ledger", "avg balance", "metric", "metric.average_deposit_ledger_balance", 0.74, "Common shorthand for average deposit balance."),
    ("syn.average_balance.deposit_collected", "average balance", "metric", "metric.average_deposit_collected_balance", 0.62, "Possible interpretation when funds availability context is present."),
    ("syn.average_balance.loan_outstanding", "average balance", "metric", "metric.average_loan_outstanding_balance", 0.58, "Possible interpretation when lending context is present."),
    ("syn.deposit_average_balance", "average deposit balance", "metric", "metric.average_deposit_ledger_balance", 0.91, "Preferred governed deposit interpretation."),
    ("syn.ledger_balance", "ledger balance", "metric", "metric.average_deposit_ledger_balance", 0.95, "Direct metric synonym."),
    ("syn.collected_balance", "collected balance", "metric", "metric.average_deposit_collected_balance", 0.95, "Direct metric synonym."),
    ("syn.available_balance", "available balance", "metric", "metric.average_available_balance", 0.94, "Direct metric synonym."),
    ("syn.total_deposits", "total deposits", "metric", "metric.total_commercial_deposits", 0.93, "Direct metric synonym."),
    ("syn.commercial_deposits", "commercial deposits", "metric", "metric.total_commercial_deposits", 0.88, "Common deposit balance phrase."),
    ("syn.loan_balance", "loan balance", "metric", "metric.total_loan_outstanding_balance", 0.88, "Common commercial lending phrase."),
    ("syn.outstanding_balance", "outstanding balance", "metric", "metric.total_loan_outstanding_balance", 0.90, "Direct loan metric synonym."),
    ("syn.loan_utilization", "loan utilization", "metric", "metric.loan_utilization_rate", 0.96, "Direct metric synonym."),
    ("syn.utilization", "utilization", "metric", "metric.loan_utilization_rate", 0.82, "Usually loan utilization in lending context."),
    ("syn.delinquency_rate", "delinquency rate", "metric", "metric.loan_delinquency_rate", 0.95, "Direct metric synonym."),
    ("syn.past_due", "past due", "metric", "metric.past_due_amount", 0.86, "Past due amount synonym."),
    ("syn.non_accrual", "non accrual", "metric", "metric.non_accrual_balance", 0.90, "Non-accrual loan balance synonym."),
    ("syn.watchlist_exposure", "watchlist exposure", "metric", "metric.watchlist_exposure", 0.91, "Direct credit risk metric synonym."),
    ("syn.relationship_profit", "relationship profit", "metric", "metric.relationship_profit", 0.94, "Direct profitability metric synonym."),
    ("syn.profitability", "profitability", "metric", "metric.relationship_profit", 0.84, "Common profitability phrase."),
    ("syn.nii", "nii", "metric", "metric.net_interest_income", 0.91, "Banking shorthand for net interest income."),
    ("syn.fee_income", "fee income", "metric", "metric.fee_income", 0.94, "Direct fee income metric synonym."),
    ("syn.segment.customer", "segment", "dimension", "dimension.customer_segment", 0.76, "Most common business meaning of segment."),
    ("syn.customer_segment", "customer segment", "dimension", "dimension.customer_segment", 0.96, "Direct dimension synonym."),
    ("syn.client_segment", "client segment", "dimension", "dimension.customer_segment", 0.94, "Customer segment synonym."),
    ("syn.relationship_segment", "relationship segment", "dimension", "dimension.customer_segment", 0.91, "Customer segment synonym."),
    ("syn.segment.product", "segment", "dimension", "dimension.product_segment", 0.60, "Possible interpretation in product context."),
    ("syn.product_segment", "product segment", "dimension", "dimension.product_segment", 0.95, "Direct product segment synonym."),
    ("syn.product", "product", "dimension", "dimension.product_type", 0.83, "Product type is the default product grouping."),
    ("syn.account_type", "account type", "dimension", "dimension.account_type", 0.94, "Deposit account grouping."),
    ("syn.loan_type", "loan type", "dimension", "dimension.loan_type", 0.95, "Commercial lending grouping."),
    ("syn.industry", "industry", "dimension", "dimension.industry", 0.96, "Direct dimension synonym."),
    ("syn.sector", "sector", "dimension", "dimension.industry", 0.82, "Industry grouping synonym."),
    ("syn.naics", "naics", "dimension", "dimension.naics_code", 0.95, "NAICS grouping synonym."),
    ("syn.revenue_band", "revenue band", "dimension", "dimension.revenue_band", 0.94, "Direct dimension synonym."),
    ("syn.region.customer", "region", "dimension", "dimension.customer_region", 0.72, "Customer relationship region interpretation."),
    ("syn.region.branch", "region", "dimension", "dimension.branch_region", 0.68, "Branch region interpretation."),
    ("syn.market", "market", "dimension", "dimension.market", 0.90, "Commercial market grouping."),
    ("syn.state", "state", "dimension", "dimension.state", 0.90, "State grouping."),
    ("syn.rm", "relationship manager", "dimension", "dimension.relationship_manager", 0.96, "Direct dimension synonym."),
    ("syn.risk_rating", "risk rating", "dimension", "dimension.risk_rating", 0.95, "Direct credit risk dimension synonym."),
    ("syn.risk_segment", "risk segment", "dimension", "dimension.risk_rating", 0.80, "Governed fallback to risk rating; no separate risk_segment column exists."),
    ("syn.delinquency_bucket", "delinquency bucket", "dimension", "dimension.delinquency_bucket", 0.95, "Direct delinquency dimension synonym."),
    ("syn.month", "month", "dimension", "dimension.year_month", 0.84, "Default time trend grouping."),
    ("syn.monthly", "monthly", "dimension", "dimension.year_month", 0.86, "Default monthly trend grouping."),
]


ACCESS_POLICIES = [
    ("policy.business.table.read", "business_user", "table", "*", "read_aggregate", None, "Business users may query governed aggregate results."),
    ("policy.business.customer_name.mask", "business_user", "column", "dim_customer.customer_name", "masked", "show_business_label_only", "Customer names are masked for business demo users unless explicitly permitted."),
    ("policy.business.account_number.deny", "business_user", "column", "dim_account.account_number", "deny", None, "Account numbers are restricted."),
    ("policy.business.loan_number.deny", "business_user", "column", "dim_loan.loan_number", "deny", None, "Loan numbers are restricted."),
    ("policy.business.sql.hidden", "business_user", "feature", "generated_sql", "hidden", None, "Generated SQL is hidden by default for business users."),
    ("policy.technical.table.read", "technical_user", "table", "*", "read", None, "Technical users may inspect governed tables and SQL."),
    ("policy.technical.customer_name.mask", "technical_user", "column", "dim_customer.customer_name", "masked", "partial_mask", "Technical users see masked customer identifiers unless admin access is granted."),
    ("policy.technical.account_number.mask", "technical_user", "column", "dim_account.account_number", "masked", "partial_mask", "Technical users see masked account numbers."),
    ("policy.technical.loan_number.mask", "technical_user", "column", "dim_loan.loan_number", "masked", "partial_mask", "Technical users see masked loan numbers."),
    ("policy.technical.sql.visible", "technical_user", "feature", "generated_sql", "visible", None, "Technical users can view generated SQL and metadata sources."),
    ("policy.admin.all", "admin", "table", "*", "read", None, "Admins may inspect all synthetic governed data."),
    ("policy.admin.sql.visible", "admin", "feature", "generated_sql", "visible", None, "Admins can view SQL and detailed lineage."),
]


def create_metadata_schema(conn: duckdb.DuckDBPyConnection) -> None:
    for table_name in METADATA_TABLE_NAMES:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.execute(
        """
        CREATE TABLE metadata_table (
            table_name VARCHAR PRIMARY KEY,
            business_name VARCHAR,
            subject_area VARCHAR,
            table_type VARCHAR,
            domain VARCHAR,
            grain VARCHAR,
            refresh_frequency VARCHAR,
            data_owner VARCHAR,
            certified_flag BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_column (
            table_name VARCHAR,
            column_name VARCHAR,
            business_name VARCHAR,
            data_type VARCHAR,
            description VARCHAR,
            semantic_type VARCHAR,
            pii_flag BOOLEAN,
            restricted_flag BOOLEAN,
            sample_values VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_business_term (
            term_id VARCHAR PRIMARY KEY,
            term_name VARCHAR,
            term_type VARCHAR,
            definition VARCHAR,
            calculation VARCHAR,
            primary_table VARCHAR,
            primary_column VARCHAR,
            certified_flag BOOLEAN,
            owner VARCHAR,
            subject_area VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_metric (
            metric_id VARCHAR PRIMARY KEY,
            metric_name VARCHAR,
            description VARCHAR,
            calculation_sql VARCHAR,
            aggregation_type VARCHAR,
            base_table VARCHAR,
            required_columns VARCHAR,
            default_time_period VARCHAR,
            certified_flag BOOLEAN,
            subject_area VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_dimension (
            dimension_id VARCHAR PRIMARY KEY,
            dimension_name VARCHAR,
            description VARCHAR,
            table_name VARCHAR,
            column_name VARCHAR,
            sample_values VARCHAR,
            certified_flag BOOLEAN,
            subject_area VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_join_path (
            join_path_id VARCHAR PRIMARY KEY,
            from_table VARCHAR,
            from_column VARCHAR,
            to_table VARCHAR,
            to_column VARCHAR,
            relationship_type VARCHAR,
            join_type VARCHAR,
            description VARCHAR,
            certified_flag BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_lineage (
            lineage_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_object VARCHAR,
            target_table VARCHAR,
            target_column VARCHAR,
            transformation VARCHAR,
            refresh_frequency VARCHAR,
            data_owner VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_synonym (
            synonym_id VARCHAR PRIMARY KEY,
            phrase VARCHAR,
            target_type VARCHAR,
            target_id VARCHAR,
            confidence DOUBLE,
            notes VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE metadata_access_policy (
            policy_id VARCHAR PRIMARY KEY,
            role_name VARCHAR,
            asset_type VARCHAR,
            asset_name VARCHAR,
            permission VARCHAR,
            masking_rule VARCHAR,
            notes VARCHAR
        )
        """
    )


def insert_rows(conn: duckdb.DuckDBPyConnection, table_name: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(rows[0]))
    conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)


def seed_governance_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    create_metadata_schema(conn)
    insert_rows(conn, "metadata_table", TABLE_METADATA)
    insert_rows(conn, "metadata_column", COLUMN_METADATA)
    insert_rows(conn, "metadata_business_term", BUSINESS_TERMS)
    insert_rows(conn, "metadata_metric", METRICS)
    insert_rows(conn, "metadata_dimension", DIMENSIONS)
    insert_rows(conn, "metadata_join_path", JOIN_PATHS)
    insert_rows(conn, "metadata_lineage", LINEAGE)
    insert_rows(conn, "metadata_synonym", SYNONYMS)
    insert_rows(conn, "metadata_access_policy", ACCESS_POLICIES)

