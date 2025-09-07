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