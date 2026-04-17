from __future__ import annotations

import argparse
import math
import random
from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

import duckdb

from backend.app.catalog.search_index import SEARCH_TABLE_NAMES, seed_search_index
from backend.app.core.config import get_settings
from backend.app.synthetic_data.metadata_seed import METADATA_TABLE_NAMES, seed_governance_metadata


DEFAULT_SEED = 20260416
START_DATE = date(2024, 1, 1)
END_DATE = date(2026, 3, 31)
CUSTOMER_COUNT = 280
TRANSACTION_COUNT = 14_000
T = TypeVar("T")


@dataclass(frozen=True)
class CustomerProfile:
    customer_id: int
    customer_name: str
    customer_segment: str
    industry: str
    naics_code: str
    revenue_band: str
    risk_rating: int
    region: str
    market: str
    state: str
    relationship_manager: str
    relationship_start_date: date


@dataclass(frozen=True)
class AccountProfile:
    account_id: int
    customer_id: int
    product_id: int
    branch_id: int
    account_number: str
    account_type: str
    open_date: date
    status: str
    interest_bearing_flag: bool
    base_balance: float


@dataclass(frozen=True)
class LoanProfile:
    loan_id: int
    customer_id: int
    product_id: int
    branch_id: int
    loan_number: str
    loan_type: str
    origination_date: date
    maturity_date: date
    commitment_amount: float
    rate_type: str
    interest_rate: float
    collateral_type: str
    status: str


INDUSTRIES: list[tuple[str, str]] = [
    ("Manufacturing", "31-33"),
    ("Healthcare Services", "62"),
    ("Professional Services", "54"),
    ("Technology", "51"),
    ("Commercial Real Estate", "53"),
    ("Construction", "23"),
    ("Wholesale Trade", "42"),
    ("Retail Trade", "44-45"),
    ("Transportation and Logistics", "48-49"),
    ("Hospitality", "72"),
    ("Energy", "21"),
    ("Agriculture", "11"),
]

REVENUE_BANDS_BY_SEGMENT: dict[str, list[str]] = {
    "Small Business": ["<$5MM", "$5MM-$10MM"],
    "Business Banking": ["$5MM-$10MM", "$10MM-$25MM"],
    "Middle Market": ["$25MM-$50MM", "$50MM-$100MM", "$100MM-$250MM"],
    "Corporate Banking": ["$250MM-$500MM", "$500MM-$1B", ">$1B"],
    "Commercial Real Estate": ["$25MM-$50MM", "$50MM-$100MM", "$100MM-$250MM", "$250MM-$500MM"],
}

SEGMENT_WEIGHTS = [
    ("Small Business", 0.22),
    ("Business Banking", 0.22),
    ("Middle Market", 0.28),
    ("Corporate Banking", 0.16),
    ("Commercial Real Estate", 0.12),
]

RISK_RATING_WEIGHTS = [
    (1, 0.08),
    (2, 0.18),
    (3, 0.31),
    (4, 0.24),
    (5, 0.12),
    (6, 0.05),
    (7, 0.02),
]

PD_BY_RATING = {
    1: 0.0015,
    2: 0.0035,
    3: 0.0080,
    4: 0.0180,
    5: 0.0450,
    6: 0.0950,
    7: 0.1850,
}

BRANCHES = [
    (1, "Phoenix Commercial Banking Center", "Southwest", "Phoenix", "AZ", "Phoenix"),
    (2, "Scottsdale Treasury Office", "Southwest", "Phoenix", "AZ", "Scottsdale"),
    (3, "Tucson Business Banking Office", "Southwest", "Tucson", "AZ", "Tucson"),
    (4, "Denver Middle Market Center", "West", "Denver", "CO", "Denver"),
    (5, "Salt Lake Commercial Office", "West", "Salt Lake City", "UT", "Salt Lake City"),
    (6, "Los Angeles Corporate Banking", "West", "Los Angeles", "CA", "Los Angeles"),
    (7, "San Diego Business Banking", "West", "San Diego", "CA", "San Diego"),
    (8, "Dallas Commercial Banking Center", "Southwest", "Dallas", "TX", "Dallas"),
    (9, "Houston Energy Banking Office", "Southwest", "Houston", "TX", "Houston"),
    (10, "Chicago Middle Market Center", "Midwest", "Chicago", "IL", "Chicago"),
    (11, "Minneapolis Business Banking", "Midwest", "Minneapolis", "MN", "Minneapolis"),
    (12, "Atlanta Commercial Office", "Southeast", "Atlanta", "GA", "Atlanta"),
    (13, "Charlotte Corporate Banking", "Southeast", "Charlotte", "NC", "Charlotte"),
    (14, "Miami Business Banking", "Southeast", "Miami", "FL", "Miami"),
    (15, "New York Corporate Banking", "Northeast", "New York", "NY", "New York"),
    (16, "Boston Commercial Banking", "Northeast", "Boston", "MA", "Boston"),
    (17, "Philadelphia Middle Market", "Northeast", "Philadelphia", "PA", "Philadelphia"),
    (18, "Seattle Commercial Office", "West", "Seattle", "WA", "Seattle"),
]

PRODUCTS = [
    (101, "CDA", "Commercial Demand Deposit", "Deposits", "DDA", "Operating Deposits", False),
    (102, "ACHK", "Analyzed Commercial Checking", "Deposits", "Checking", "Operating Deposits", False),
    (103, "MMDA", "Commercial Money Market", "Deposits", "Money Market", "Interest Bearing Deposits", True),
    (104, "BSAV", "Business Savings", "Deposits", "Savings", "Interest Bearing Deposits", True),
    (105, "SWEEP", "Commercial Sweep Account", "Deposits", "Sweep", "Liquidity Management", True),
    (201, "RLOC", "Commercial Revolving Line", "Loans", "Revolver", "C&I Lending", False),
    (202, "TERM", "Commercial Term Loan", "Loans", "Term Loan", "C&I Lending", False),
    (203, "CRE", "Commercial Real Estate Mortgage", "Loans", "CRE Loan", "Commercial Real Estate", False),
    (204, "EQF", "Equipment Finance", "Loans", "Equipment Finance", "Specialty Lending", False),
    (205, "SBA", "SBA Commercial Loan", "Loans", "SBA Loan", "Small Business Lending", False),
    (206, "ABL", "Asset Based Lending Facility", "Loans", "ABL", "C&I Lending", False),
]

RELATIONSHIP_MANAGERS = [
    "Avery Morgan",
    "Blake Chen",
    "Cameron Patel",
    "Devon Williams",
    "Emerson Brooks",
    "Finley Garcia",
    "Harper Johnson",
    "Jordan Lee",
    "Kendall Martin",
    "Logan Rivera",
    "Morgan Taylor",
    "Parker Nguyen",
    "Quinn Thompson",
    "Riley Davis",
    "Sawyer Adams",
]

COMPANY_PREFIXES = [
    "Summit",
    "Pioneer",
    "Keystone",
    "Northstar",
    "Redwood",
    "Sterling",
    "Evergreen",
    "Canyon",
    "Bluewater",
    "Silverline",
    "Meridian",
    "Harbor",
    "Ironwood",
    "Clearview",
    "Crescent",
    "Frontier",
    "Westbridge",
    "Granite",
]

COMPANY_NOUNS = [
    "Logistics",
    "Health Partners",
    "Manufacturing",
    "Foods",
    "Energy Services",
    "Retail Group",
    "Builders",
    "Software",
    "Properties",
    "Distribution",
    "Advisors",
    "Medical Systems",
    "Industrial Supply",
    "Hospitality",
    "Equipment",
    "Construction",
]

COMPANY_SUFFIXES = ["LLC", "Inc.", "Group", "Holdings", "Partners", "Corporation", "Co."]


def weighted_choice(rng: random.Random, choices: list[tuple[T, float]]) -> T:
    threshold = rng.random()
    cumulative = 0.0
    for value, weight in choices:
        cumulative += weight
        if threshold <= cumulative:
            return value
    return choices[-1][0]


def random_date(rng: random.Random, start: date, end: date) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=rng.randint(0, delta_days))


def iter_dates(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def last_day_of_month(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def iter_month_ends(start: date, end: date) -> list[date]:
    months: list[date] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        month_end = last_day_of_month(cursor.year, cursor.month)
        if month_end >= start and month_end <= end:
            months.append(month_end)
        next_month = cursor.month + 1
        next_year = cursor.year
        if next_month == 13:
            next_month = 1
            next_year += 1
        cursor = date(next_year, next_month, 1)
    return months


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    tables = [
        "fact_relationship_profitability",
        "fact_credit_risk_snapshot",
        "fact_loan_payment",
        "fact_loan_balance_monthly",
        "fact_deposit_transaction",
        "fact_deposit_balance_daily",
        "dim_loan",
        "dim_account",
        "dim_customer",
        "dim_product",
        "dim_branch",
        "dim_date",
        "data_generation_summary",
    ]
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    conn.execute(
        """
        CREATE TABLE dim_date (
            calendar_date DATE PRIMARY KEY,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name VARCHAR,
            year_month VARCHAR,
            day_of_month INTEGER,
            day_of_week INTEGER,
            is_business_day BOOLEAN,
            is_month_end BOOLEAN,
            fiscal_quarter VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE dim_branch (
            branch_id INTEGER PRIMARY KEY,
            branch_name VARCHAR,
            region VARCHAR,
            market VARCHAR,
            state VARCHAR,
            city VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE dim_product (
            product_id INTEGER PRIMARY KEY,
            product_code VARCHAR,
            product_name VARCHAR,
            product_family VARCHAR,
            product_type VARCHAR,
            product_segment VARCHAR,
            interest_bearing_flag BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE dim_customer (
            customer_id INTEGER PRIMARY KEY,
            customer_name VARCHAR,
            customer_segment VARCHAR,
            industry VARCHAR,
            naics_code VARCHAR,
            revenue_band VARCHAR,
            risk_rating INTEGER,
            region VARCHAR,
            market VARCHAR,
            state VARCHAR,
            relationship_manager VARCHAR,
            relationship_start_date DATE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE dim_account (
            account_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            branch_id INTEGER,
            account_number VARCHAR,
            account_type VARCHAR,
            open_date DATE,
            status VARCHAR,
            interest_bearing_flag BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE dim_loan (
            loan_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            branch_id INTEGER,
            loan_number VARCHAR,
            loan_type VARCHAR,
            origination_date DATE,
            maturity_date DATE,
            commitment_amount DOUBLE,
            rate_type VARCHAR,
            interest_rate DOUBLE,
            collateral_type VARCHAR,
            status VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_deposit_balance_daily (
            as_of_date DATE,
            account_id INTEGER,
            ledger_balance DOUBLE,
            collected_balance DOUBLE,
            available_balance DOUBLE,
            average_balance_mtd DOUBLE,
            interest_rate DOUBLE,
            accrued_interest_mtd DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_deposit_transaction (
            transaction_id INTEGER PRIMARY KEY,
            transaction_date DATE,
            account_id INTEGER,
            transaction_type VARCHAR,
            channel VARCHAR,
            direction VARCHAR,
            amount DOUBLE,
            description VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_loan_balance_monthly (
            as_of_month DATE,
            loan_id INTEGER,
            outstanding_balance DOUBLE,
            commitment_amount DOUBLE,
            available_credit DOUBLE,
            utilization_rate DOUBLE,
            past_due_amount DOUBLE,
            delinquency_bucket VARCHAR,
            non_accrual_flag BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_loan_payment (
            payment_id INTEGER PRIMARY KEY,
            payment_date DATE,
            loan_id INTEGER,
            principal_amount DOUBLE,
            interest_amount DOUBLE,
            fee_amount DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_credit_risk_snapshot (
            as_of_month DATE,
            customer_id INTEGER,
            risk_rating INTEGER,
            probability_of_default DOUBLE,
            loss_given_default DOUBLE,
            exposure_at_default DOUBLE,
            watchlist_flag BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fact_relationship_profitability (
            as_of_month DATE,
            customer_id INTEGER,
            net_interest_income DOUBLE,
            fee_income DOUBLE,
            allocated_expense DOUBLE,
            provision_expense DOUBLE,
            relationship_profit DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE data_generation_summary (
            generated_at TIMESTAMP,
            seed INTEGER,
            table_name VARCHAR,
            row_count INTEGER
        )
        """
    )


def build_dim_date() -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for calendar_date in iter_dates(START_DATE, END_DATE):
        quarter = ((calendar_date.month - 1) // 3) + 1
        rows.append(
            (
                calendar_date,
                calendar_date.year,
                quarter,
                calendar_date.month,
                calendar_date.strftime("%B"),
                calendar_date.strftime("%Y-%m"),
                calendar_date.day,
                calendar_date.isoweekday(),
                calendar_date.weekday() < 5,
                calendar_date == last_day_of_month(calendar_date.year, calendar_date.month),
                f"FY{calendar_date.year} Q{quarter}",
            )
        )
    return rows


def build_customers(rng: random.Random) -> list[CustomerProfile]:
    customers: list[CustomerProfile] = []
    used_names: set[str] = set()

    for customer_id in range(1, CUSTOMER_COUNT + 1):
        segment = weighted_choice(rng, SEGMENT_WEIGHTS)
        industry, naics_code = rng.choice(INDUSTRIES)
        branch = rng.choice(BRANCHES)
        revenue_band = rng.choice(REVENUE_BANDS_BY_SEGMENT[segment])
        risk_rating = weighted_choice(rng, RISK_RATING_WEIGHTS)
        relationship_start_date = random_date(rng, date(2014, 1, 1), date(2025, 12, 31))

        name = f"{rng.choice(COMPANY_PREFIXES)} {rng.choice(COMPANY_NOUNS)} {rng.choice(COMPANY_SUFFIXES)}"
        while name in used_names:
            name = f"{rng.choice(COMPANY_PREFIXES)} {rng.choice(COMPANY_NOUNS)} {rng.choice(COMPANY_SUFFIXES)}"
        used_names.add(name)

        customers.append(
            CustomerProfile(
                customer_id=customer_id,
                customer_name=name,
                customer_segment=segment,
                industry=industry,
                naics_code=naics_code,
                revenue_band=revenue_band,
                risk_rating=risk_rating,
                region=branch[2],
                market=branch[3],
                state=branch[4],
                relationship_manager=rng.choice(RELATIONSHIP_MANAGERS),
                relationship_start_date=relationship_start_date,
            )
        )

    return customers


def build_accounts(rng: random.Random, customers: list[CustomerProfile]) -> list[AccountProfile]:
    deposit_products = [product for product in PRODUCTS if product[3] == "Deposits"]
    segment_balance_ranges = {
        "Small Business": (35_000, 450_000),
        "Business Banking": (150_000, 1_500_000),
        "Middle Market": (600_000, 7_500_000),
        "Corporate Banking": (2_000_000, 35_000_000),
        "Commercial Real Estate": (400_000, 8_500_000),
    }

    accounts: list[AccountProfile] = []
    account_id = 1

    for customer in customers:
        account_count = rng.choices([1, 2, 3, 4], weights=[0.34, 0.40, 0.20, 0.06], k=1)[0]
        for _ in range(account_count):
            product = rng.choice(deposit_products)
            branch = rng.choice(BRANCHES)
            low, high = segment_balance_ranges[customer.customer_segment]
            base_balance = rng.triangular(low, high, low + ((high - low) * 0.35))
            open_date = random_date(rng, date(2018, 1, 1), date(2025, 11, 30))
            status = rng.choices(["Active", "Dormant", "Closed"], weights=[0.94, 0.04, 0.02], k=1)[0]
            accounts.append(
                AccountProfile(
                    account_id=account_id,
                    customer_id=customer.customer_id,
                    product_id=product[0],
                    branch_id=branch[0],
                    account_number=f"DDA{account_id:09d}",
                    account_type=product[4],
                    open_date=open_date,
                    status=status,
                    interest_bearing_flag=product[6],
                    base_balance=base_balance,
                )
            )
            account_id += 1

    return accounts


def build_loans(rng: random.Random, customers: list[CustomerProfile]) -> list[LoanProfile]:
    loan_products = [product for product in PRODUCTS if product[3] == "Loans"]
    commitment_ranges = {
        "Small Business": (75_000, 900_000),
        "Business Banking": (250_000, 2_500_000),
        "Middle Market": (1_000_000, 15_000_000),
        "Corporate Banking": (5_000_000, 85_000_000),
        "Commercial Real Estate": (1_500_000, 45_000_000),
    }
    collateral_by_type = {
        "Revolver": "Accounts Receivable",
        "Term Loan": "Business Assets",
        "CRE Loan": "Commercial Real Estate",
        "Equipment Finance": "Equipment",
        "SBA Loan": "SBA Guarantee",
        "ABL": "Borrowing Base",
    }

    loans: list[LoanProfile] = []
    loan_id = 1

    for customer in customers:
        has_loan = rng.random() < {
            "Small Business": 0.42,
            "Business Banking": 0.58,
            "Middle Market": 0.72,
            "Corporate Banking": 0.78,
            "Commercial Real Estate": 0.86,
        }[customer.customer_segment]
        if not has_loan:
            continue

        loan_count = rng.choices([1, 2, 3], weights=[0.72, 0.23, 0.05], k=1)[0]
        for _ in range(loan_count):
            product = rng.choice(loan_products)
            if customer.customer_segment == "Commercial Real Estate":
                product = rng.choice([product for product in loan_products if product[4] in {"CRE Loan", "Term Loan", "Revolver"}])
            branch = rng.choice(BRANCHES)
            low, high = commitment_ranges[customer.customer_segment]
            commitment_amount = round(rng.triangular(low, high, low + ((high - low) * 0.28)), 2)
            origination_date = random_date(rng, date(2020, 1, 1), date(2025, 12, 31))
            term_years = rng.choice([2, 3, 5, 7, 10])
            maturity_date = date(origination_date.year + term_years, origination_date.month, min(origination_date.day, 28))
            rate_type = rng.choices(["Fixed", "Variable"], weights=[0.38, 0.62], k=1)[0]
            base_rate = 0.052 if rate_type == "Fixed" else 0.061
            risk_spread = customer.risk_rating * rng.uniform(0.002, 0.006)
            interest_rate = round(base_rate + risk_spread + rng.uniform(-0.004, 0.004), 5)
            status = rng.choices(["Active", "Matured", "Charged Off"], weights=[0.93, 0.05, 0.02], k=1)[0]

            loans.append(
                LoanProfile(
                    loan_id=loan_id,
                    customer_id=customer.customer_id,
                    product_id=product[0],
                    branch_id=branch[0],
                    loan_number=f"LN{loan_id:09d}",
                    loan_type=product[4],
                    origination_date=origination_date,
                    maturity_date=maturity_date,
                    commitment_amount=commitment_amount,
                    rate_type=rate_type,
                    interest_rate=interest_rate,
                    collateral_type=collateral_by_type[product[4]],
                    status=status,
                )
            )
            loan_id += 1

    return loans


def build_deposit_balances(
    rng: random.Random,
    accounts: list[AccountProfile],
) -> list[tuple[Any, ...]]:
    business_dates = [day for day in iter_dates(START_DATE, END_DATE) if day.weekday() < 5]
    rows: list[tuple[Any, ...]] = []

    for account in accounts:
        if account.status == "Closed":
            active_end_index = int(len(business_dates) * rng.uniform(0.45, 0.95))
        else:
            active_end_index = len(business_dates)

        ledger_balance = account.base_balance * rng.uniform(0.80, 1.20)
        annual_growth = rng.normalvariate(0.025, 0.035)
        month_running_total = 0.0
        month_running_days = 0
        current_month = None

        for day_index, as_of_date in enumerate(business_dates[:active_end_index]):
            if as_of_date < account.open_date:
                continue

            if current_month != (as_of_date.year, as_of_date.month):
                current_month = (as_of_date.year, as_of_date.month)
                month_running_total = 0.0
                month_running_days = 0

            seasonal = 1.0 + 0.035 * math.sin((2 * math.pi * as_of_date.timetuple().tm_yday) / 365)
            elapsed_years = max(0, (as_of_date - START_DATE).days) / 365
            trend = max(0.80, 1.0 + (annual_growth * elapsed_years))
            month_end_boost = 1.025 if as_of_date.day >= 25 else 1.0
            target_balance = account.base_balance * seasonal * trend * month_end_boost
            daily_noise = rng.normalvariate(0, 0.012)
            ledger_balance = max(500.0, ((ledger_balance * 0.88) + (target_balance * 0.12)) * (1 + daily_noise))

            if rng.random() < 0.035:
                ledger_balance *= rng.uniform(0.82, 1.18)

            collected_balance = ledger_balance * rng.uniform(0.925, 0.998)
            available_balance = collected_balance * rng.uniform(0.90, 0.99)
            interest_rate = rng.uniform(0.0025, 0.035) if account.interest_bearing_flag else 0.0

            month_running_total += ledger_balance
            month_running_days += 1
            average_balance_mtd = month_running_total / month_running_days
            accrued_interest_mtd = average_balance_mtd * interest_rate * month_running_days / 365

            rows.append(
                (
                    as_of_date,
                    account.account_id,
                    round(ledger_balance, 2),
                    round(collected_balance, 2),
                    round(available_balance, 2),
                    round(average_balance_mtd, 2),
                    round(interest_rate, 5),
                    round(accrued_interest_mtd, 2),
                )
            )

            if day_index % 22 == 0:
                ledger_balance = (ledger_balance * 0.88) + (target_balance * 0.12)

    return rows


def build_deposit_transactions(
    rng: random.Random,
    accounts: list[AccountProfile],
) -> list[tuple[Any, ...]]:
    credit_types = [
        ("ACH Credit", "ACH", "Credit", "Customer ACH credit"),
        ("Wire In", "Wire", "Credit", "Incoming commercial wire"),
        ("Remote Deposit", "Digital", "Credit", "Remote deposit capture"),
        ("Lockbox Credit", "Lockbox", "Credit", "Wholesale lockbox deposit"),
    ]
    debit_types = [
        ("ACH Debit", "ACH", "Debit", "Vendor ACH payment"),
        ("Wire Out", "Wire", "Debit", "Outgoing commercial wire"),
        ("Check Paid", "Branch", "Debit", "Commercial check paid"),
        ("Treasury Fee", "Treasury", "Debit", "Treasury management fee"),
    ]
    rows: list[tuple[Any, ...]] = []

    active_accounts = [account for account in accounts if account.status != "Closed"]
    for transaction_id in range(1, TRANSACTION_COUNT + 1):
        account = rng.choice(active_accounts)
        transaction_date = random_date(rng, max(account.open_date, START_DATE), END_DATE)
        if transaction_date.weekday() >= 5:
            transaction_date -= timedelta(days=transaction_date.weekday() - 4)

        is_credit = rng.random() < 0.53
        transaction_type, channel, direction, description = rng.choice(credit_types if is_credit else debit_types)

        amount_scale = max(account.base_balance * rng.uniform(0.004, 0.10), 250)
        if transaction_type == "Treasury Fee":
            amount = rng.uniform(25, 2_500)
        else:
            amount = rng.triangular(100, amount_scale, amount_scale * 0.25)

        rows.append(
            (
                transaction_id,
                transaction_date,
                account.account_id,
                transaction_type,
                channel,
                direction,
                round(amount, 2),
                description,
            )
        )

    return rows


def build_loan_balances(
    rng: random.Random,
    loans: list[LoanProfile],
    customer_by_id: dict[int, CustomerProfile],
) -> list[tuple[Any, ...]]:
    month_ends = iter_month_ends(START_DATE, END_DATE)
    utilization_base_by_type = {
        "Revolver": 0.46,
        "Term Loan": 0.84,
        "CRE Loan": 0.78,
        "Equipment Finance": 0.72,
        "SBA Loan": 0.69,
        "ABL": 0.58,
    }
    rows: list[tuple[Any, ...]] = []

    for loan in loans:
        customer = customer_by_id[loan.customer_id]
        for month_index, as_of_month in enumerate(month_ends):
            if as_of_month < loan.origination_date or as_of_month > loan.maturity_date:
                continue

            months_since_start = max(0, (as_of_month.year - loan.origination_date.year) * 12 + as_of_month.month - loan.origination_date.month)
            amortization = max(0.58, 1.0 - (months_since_start * 0.0055)) if loan.loan_type != "Revolver" else 1.0
            seasonal = 1.0 + 0.06 * math.sin((month_index + loan.loan_id % 5) / 12 * 2 * math.pi)
            risk_factor = 1.0 + ((customer.risk_rating - 3) * 0.025)
            utilization = utilization_base_by_type[loan.loan_type] * seasonal * risk_factor + rng.normalvariate(0, 0.055)
            utilization = min(max(utilization, 0.08), 0.98)
            outstanding_balance = loan.commitment_amount * utilization * amortization
            available_credit = max(0.0, loan.commitment_amount - outstanding_balance)

            delinquency_probability = max(0.005, customer.risk_rating * 0.014)
            is_delinquent = rng.random() < delinquency_probability
            if not is_delinquent:
                delinquency_bucket = "Current"
                past_due_amount = 0.0
            else:
                delinquency_bucket = rng.choices(
                    ["1-29 DPD", "30-59 DPD", "60-89 DPD", "90+ DPD"],
                    weights=[0.58, 0.25, 0.12, 0.05],
                    k=1,
                )[0]
                past_due_amount = outstanding_balance * rng.uniform(0.005, 0.065)

            non_accrual_flag = delinquency_bucket == "90+ DPD" or (customer.risk_rating >= 6 and rng.random() < 0.06)

            rows.append(
                (
                    as_of_month,
                    loan.loan_id,
                    round(outstanding_balance, 2),
                    round(loan.commitment_amount, 2),
                    round(available_credit, 2),
                    round(outstanding_balance / loan.commitment_amount, 5),
                    round(past_due_amount, 2),
                    delinquency_bucket,
                    non_accrual_flag,
                )
            )

    return rows


def build_loan_payments(
    rng: random.Random,
    loan_balances: list[tuple[Any, ...]],
    loan_by_id: dict[int, LoanProfile],
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    payment_id = 1

    for as_of_month, loan_id, outstanding_balance, *_ in loan_balances:
        loan = loan_by_id[loan_id]
        payment_date = date(as_of_month.year, as_of_month.month, min(15 + rng.randint(-3, 3), monthrange(as_of_month.year, as_of_month.month)[1]))
        principal_rate = rng.uniform(0.002, 0.018) if loan.loan_type != "Revolver" else rng.uniform(0.0, 0.006)
        principal_amount = outstanding_balance * principal_rate
        interest_amount = outstanding_balance * loan.interest_rate / 12
        fee_amount = rng.uniform(0, 450) if rng.random() < 0.30 else 0.0

        rows.append(
            (
                payment_id,
                payment_date,
                loan_id,
                round(principal_amount, 2),
                round(interest_amount, 2),
                round(fee_amount, 2),
            )
        )
        payment_id += 1

    return rows


def build_credit_risk_snapshots(
    rng: random.Random,
    customers: list[CustomerProfile],
    loans: list[LoanProfile],
) -> list[tuple[Any, ...]]:
    commitments_by_customer: defaultdict[int, float] = defaultdict(float)
    for loan in loans:
        commitments_by_customer[loan.customer_id] += loan.commitment_amount

    rows: list[tuple[Any, ...]] = []
    month_ends = iter_month_ends(START_DATE, END_DATE)

    for customer in customers:
        current_rating = customer.risk_rating
        for as_of_month in month_ends:
            if rng.random() < 0.035:
                current_rating = min(7, max(1, current_rating + rng.choice([-1, 1])))

            pd = PD_BY_RATING[current_rating] * rng.uniform(0.85, 1.25)
            lgd = rng.uniform(0.28, 0.58)
            exposure = commitments_by_customer[customer.customer_id] * rng.uniform(0.55, 0.96)
            if exposure == 0:
                exposure = rng.uniform(25_000, 400_000) if customer.customer_segment in {"Small Business", "Business Banking"} else rng.uniform(250_000, 2_500_000)

            rows.append(
                (
                    as_of_month,
                    customer.customer_id,
                    current_rating,
                    round(pd, 5),
                    round(lgd, 5),
                    round(exposure, 2),
                    current_rating >= 5,
                )
            )

    return rows


def build_relationship_profitability(
    rng: random.Random,
    customers: list[CustomerProfile],
    accounts: list[AccountProfile],
    loans: list[LoanProfile],
) -> list[tuple[Any, ...]]:
    deposit_base_by_customer: defaultdict[int, float] = defaultdict(float)
    commitment_by_customer: defaultdict[int, float] = defaultdict(float)

    for account in accounts:
        deposit_base_by_customer[account.customer_id] += account.base_balance
    for loan in loans:
        commitment_by_customer[loan.customer_id] += loan.commitment_amount

    rows: list[tuple[Any, ...]] = []
    month_ends = iter_month_ends(START_DATE, END_DATE)

    for customer in customers:
        for month_index, as_of_month in enumerate(month_ends):
            seasonality = 1.0 + 0.05 * math.sin((month_index / 12) * 2 * math.pi)
            deposit_spread_income = deposit_base_by_customer[customer.customer_id] * rng.uniform(0.0012, 0.0038) * seasonality
            loan_spread_income = commitment_by_customer[customer.customer_id] * rng.uniform(0.0018, 0.0065)
            net_interest_income = deposit_spread_income + loan_spread_income
            fee_income = (
                deposit_base_by_customer[customer.customer_id] * rng.uniform(0.0002, 0.0012)
                + commitment_by_customer[customer.customer_id] * rng.uniform(0.00005, 0.00045)
            )
            allocated_expense = max(1_000.0, (net_interest_income + fee_income) * rng.uniform(0.22, 0.45))
            provision_expense = commitment_by_customer[customer.customer_id] * PD_BY_RATING[customer.risk_rating] * rng.uniform(0.10, 0.26)
            relationship_profit = net_interest_income + fee_income - allocated_expense - provision_expense

            rows.append(
                (
                    as_of_month,
                    customer.customer_id,
                    round(net_interest_income, 2),
                    round(fee_income, 2),
                    round(allocated_expense, 2),
                    round(provision_expense, 2),
                    round(relationship_profit, 2),
                )
            )

    return rows


def insert_rows(conn: duckdb.DuckDBPyConnection, table_name: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(rows[0]))
    conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)


def write_generation_summary(conn: duckdb.DuckDBPyConnection, seed: int) -> dict[str, int]:
    table_names = [
        "dim_date",
        "dim_branch",
        "dim_product",
        "dim_customer",
        "dim_account",
        "dim_loan",
        "fact_deposit_balance_daily",
        "fact_deposit_transaction",
        "fact_loan_balance_monthly",
        "fact_loan_payment",
        "fact_credit_risk_snapshot",
        "fact_relationship_profitability",
        *reversed(METADATA_TABLE_NAMES),
        *SEARCH_TABLE_NAMES,
    ]
    generated_at = datetime.utcnow()
    summary: dict[str, int] = {}
    rows: list[tuple[Any, ...]] = []

    for table_name in table_names:
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        summary[table_name] = row_count
        rows.append((generated_at, seed, table_name, row_count))

    insert_rows(conn, "data_generation_summary", rows)
    return summary


def generate_database(output_path: Path, seed: int = DEFAULT_SEED, replace: bool = True) -> dict[str, int]:
    rng = random.Random(seed)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if replace and output_path.exists():
        output_path.unlink()

    with duckdb.connect(str(output_path)) as conn:
        create_schema(conn)

        customers = build_customers(rng)
        accounts = build_accounts(rng, customers)
        loans = build_loans(rng, customers)
        customer_by_id = {customer.customer_id: customer for customer in customers}
        loan_by_id = {loan.loan_id: loan for loan in loans}

        insert_rows(conn, "dim_date", build_dim_date())
        insert_rows(conn, "dim_branch", BRANCHES)
        insert_rows(conn, "dim_product", PRODUCTS)
        insert_rows(conn, "dim_customer", [tuple(customer.__dict__.values()) for customer in customers])
        insert_rows(
            conn,
            "dim_account",
            [
                (
                    account.account_id,
                    account.customer_id,
                    account.product_id,
                    account.branch_id,
                    account.account_number,
                    account.account_type,
                    account.open_date,
                    account.status,
                    account.interest_bearing_flag,
                )
                for account in accounts
            ],
        )
        insert_rows(
            conn,
            "dim_loan",
            [
                (
                    loan.loan_id,
                    loan.customer_id,
                    loan.product_id,
                    loan.branch_id,
                    loan.loan_number,
                    loan.loan_type,
                    loan.origination_date,
                    loan.maturity_date,
                    loan.commitment_amount,
                    loan.rate_type,
                    loan.interest_rate,
                    loan.collateral_type,
                    loan.status,
                )
                for loan in loans
            ],
        )

        deposit_balances = build_deposit_balances(rng, accounts)
        deposit_transactions = build_deposit_transactions(rng, accounts)
        loan_balances = build_loan_balances(rng, loans, customer_by_id)
        loan_payments = build_loan_payments(rng, loan_balances, loan_by_id)
        risk_snapshots = build_credit_risk_snapshots(rng, customers, loans)
        relationship_profitability = build_relationship_profitability(rng, customers, accounts, loans)

        insert_rows(conn, "fact_deposit_balance_daily", deposit_balances)
        insert_rows(conn, "fact_deposit_transaction", deposit_transactions)
        insert_rows(conn, "fact_loan_balance_monthly", loan_balances)
        insert_rows(conn, "fact_loan_payment", loan_payments)
        insert_rows(conn, "fact_credit_risk_snapshot", risk_snapshots)
        insert_rows(conn, "fact_relationship_profitability", relationship_profitability)
        seed_governance_metadata(conn)
        seed_search_index(conn)

        return write_generation_summary(conn, seed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic commercial banking data.")
    parser.add_argument(
        "--output",
        type=Path,
        default=get_settings().duckdb_path,
        help="Output DuckDB path.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Deterministic random seed.")
    parser.add_argument("--keep-existing", action="store_true", help="Do not replace an existing database file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = generate_database(args.output, seed=args.seed, replace=not args.keep_existing)
    print(f"Generated synthetic banking database: {args.output}")
    for table_name, row_count in summary.items():
        print(f"{table_name}: {row_count:,}")


if __name__ == "__main__":
    main()
