# Market Multiples Valuation Script
#
# This script performs a relative valuation of companies by comparing them to a set of peers
# using key market multiples like EV/EBITDA, P/E, and P/S. It fetches financial data from Yahoo Finance,
# calculates average peer multiples, and then applies these to the target company to
# estimate its fair value.
#
# Author: Denis Beliaev
# Date: September 2025
# License: MIT
#
# To install the required libraries, run:
# pip install yfinance pandas
#
from typing import Dict, List, Optional, Tuple
from functools import lru_cache
import warnings
import math
import yfinance as yf
import pandas as pd

# Suppress warnings from yfinance, which can sometimes be noisy
warnings.filterwarnings("ignore", category=FutureWarning)

# --- Configuration ---
# A dictionary mapping each target company to its list of comparable peers.
# The keys are the tickers of the companies to be valued, and the values are
# lists of tickers for their peers. The comments specify the industry for clarity.
companies_to_value = {
    # Non-alcoholic Beverages (The Coca-Cola Company)
    "KO": ["PEP", "KDP", "MNST"],

    # Aerospace & Defense (Airbus SE)
    "AIR.PA": ["RTX", "LMT", "NOC", "SAF.PA", "BA"],

    # Defense (Rheinmetall AG)
    "RHM.DE": ["NOC", "RTX", "GD", "LHX", "SAF.PA", "LMT"],

    # Defense (Lockheed Martin)
    "LMT": ["NOC", "RTX", "GD", "LHX", "SAF.PA"],

    # Tobacco (British American Tobacco)
    "BTI": ["PM", "MO"],

    # Discount Stores / Retail (Walmart)
    "WMT": ["TGT", "COST", "BJ", "KR"],

    # Discount Stores / Retail (Costco)
    "COST": ["WMT", "TGT", "BJ", "KR"],

    # Semiconductors (Nvidia)
    "NVDA": ["AVGO", "AMD", "TSM", "INTC", "QCOM", "ASML.AS", "MU"],

    # Conglomerate (Berkshire Hathaway)
    "BRK-B": ["BLK", "MKL", "L", "BN", "JEF"],

    # Software (Microsoft)
    "MSFT": ["ORCL", "SAP.DE", "CRM", "ADBE", "NOW"],

    # Fast Food Restaurants (McDonald's)
    "MCD": ["SBUX", "YUM", "CMG", "QSR", "DPZ"],

    # Financial Services / Banks (HDFC Bank)
    "HDB": ["IBN", "BBD", "ITUB", "SHG", "SID.F", "KB"],

    # Restaurants / Coffee Shops (Starbucks)
    "SBUX": ["MCD", "CMG", "QSR"],

    # Technology / Consumer Electronics (Apple)
    "AAPL": ["MSFT", "GOOGL", "SONY", "SMSN.IL", "HPQ", "DELL", "LNVGY"],

    # Consumer Goods / Food (Nestle)
    "NESN.SW": ["UL", "RBGLY", "DANOY", "MDLZ"]
}

# --- Data Preparation ---
try:
    # Read the CSV file containing deal information
    df = pd.read_csv('sample_deals_yahoo.csv')
except FileNotFoundError:
    print("✗ Error: The file 'sample_deals_yahoo.csv' was not found.")
    exit()

# Define the required columns and check for their existence
required_columns = ['ticker', 'price_ev_w', 'price_pe_w', 'price_ps_w']
if not all(col in df.columns for col in required_columns):
    missing_cols = [col for col in required_columns if col not in df.columns]
    print(f"✗ Error: The following required columns are missing from the file: {', '.join(missing_cols)}")
    exit()

# Select only the necessary columns and drop rows with missing values
combined_df = df[required_columns].dropna()

# Convert numerical columns to float, handling potential comma-based decimals
try:
    for col in ['price_ev_w', 'price_pe_w', 'price_ps_w']:
        combined_df.loc[:, col] = combined_df[col].astype(str).str.replace(',', '.').astype(float)
except ValueError as e:
    print(f"✗ Error converting data to numeric format: {e}")
    print("Please ensure that the 'price_ev_w', 'price_pe_w', and 'price_ps_w' columns contain only numerical values.")
    exit()

# Convert the DataFrame to a dictionary mapping tickers to their weights.
# The weights are used to determine the importance of each valuation method.
weights = {deal[0]: deal[1:] for deal in combined_df.drop_duplicates().values.tolist()}

# A new dictionary to store only the companies that have corresponding data in the CSV file
companies_to_evaluate = {}
for deal in combined_df.drop_duplicates().values.tolist():
    ticker = deal[0]
    if ticker in companies_to_value:
        companies_to_evaluate[ticker] = companies_to_value[ticker]
    else:
        print(f"Ticker {ticker} is not in the peer valuation database.")
        continue

# --- Core Functions ---

@lru_cache(maxsize=None)
def get_exchange_rate(currency_from: str, currency_to: str) -> Optional[float]:
    """Fetches the exchange rate between two currencies using yfinance."""
    if currency_from == currency_to:
        return 1.0

    ticker = f"{currency_from}{currency_to}=X"
    try:
        data = yf.Ticker(ticker)
        rate = data.info.get('regularMarketPrice')
        if rate:
            return rate
    except Exception as e:
        print(f"✗ Failed to get exchange rate for {ticker}: {e}")
    return None

def safe_float(value) -> Optional[float]:
    """Safely converts a value to a float, returning None on failure."""
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None

def get_value(data: pd.DataFrame, keys: List[str]) -> Optional[float]:
    """Retrieves a value from a financial data DataFrame using a list of potential keys."""
    try:
        if data is None or data.empty:
            return None

        # Ensure keys is always a list
        keys = [keys] if isinstance(keys, str) else keys

        for k in keys:
            if k in data.index:
                try:
                    series = data.loc[k].dropna()
                    if not series.empty:
                        return safe_float(series.iloc[0])
                except:
                    continue
        return None
    except Exception:
        return None

def get_company_data(ticker: str, verbose: bool = True) -> dict:
    """
    Fetches key financial data for a company from Yahoo Finance and converts it to EUR.

    Args:
        ticker: The company's ticker symbol.
        verbose: If True, prints status updates.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        # Determine the company's base currency and get the exchange rate to EUR
        currency = info.get('currency', 'USD')
        euro_rate = get_exchange_rate(currency, 'EUR')
        if not euro_rate:
            if verbose:
                print(f"✗ Could not get the exchange rate for {currency}. Using original data.")
            euro_rate = 1.0

        balance_sheet = t.balance_sheet
        financials = t.financials

        # Get the core data points
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        shares = info.get('sharesOutstanding')
        debt = info.get('totalDebt') or get_value(balance_sheet, ["Total Debt"])
        cash = info.get('cash') or get_value(balance_sheet, ["Cash And Cash Equivalents", "Cash"])
        ebitda = info.get('ebitda') or get_value(financials, ["EBITDA", "Ebitda"]) or get_value(financials, ["EBIT"])
        revenue = info.get('totalRevenue') or get_value(financials, ["Total Revenue"])
        eps = info.get('trailingEps')

        # Convert to float and apply the EUR exchange rate
        price_f = safe_float(price)
        shares_f = safe_float(shares)

        debt_f = safe_float(debt) * euro_rate if safe_float(debt) is not None else None
        cash_f = safe_float(cash) * euro_rate if safe_float(cash) is not None else None
        ebitda_f = safe_float(ebitda) * euro_rate if safe_float(ebitda) is not None else None
        revenue_f = safe_float(revenue) * euro_rate if safe_float(revenue) is not None else None
        eps_f = safe_float(eps) * euro_rate if safe_float(eps) is not None else None

        return {
            'price': price_f,
            'shares': shares_f,
            'debt': debt_f,
            'cash': cash_f,
            'ebitda': ebitda_f,
            'revenue': revenue_f,
            'eps': eps_f,
            'name': info.get('longName', ticker),
            'success': True
        }
    except Exception as e:
        if verbose:
            print(f"✗ Error fetching data for {ticker}: {e}")
        return {'success': False, 'ticker': ticker}
