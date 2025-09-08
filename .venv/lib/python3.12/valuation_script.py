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

