"""
ETF Fair Value Calculator
=========================

A comprehensive tool for calculating the fair value of Exchange Traded Funds (ETFs)
using bottom-up, peer-based valuation methodology.

Methodology Overview:
    - Values each ETF constituent individually using comparable company analysis
    - Applies multiple valuation methods (EV/EBITDA, P/E, P/S) with configurable weights
    - Aggregates individual fair values to derive ETF-level valuation
    - Provides premium/discount analysis relative to current market prices

Key Features:
    - Multi-currency support with automatic EUR normalization
    - Parallel data fetching for enhanced performance
    - Intelligent weight redistribution for missing data
    - Comprehensive error handling and diagnostics
    - Caching mechanism for efficient repeated calculations

Data Sources:
    - Yahoo Finance API for real-time financial data
    - Custom CSV files for ETF composition and weights

Usage:
    1. Configure etf_dict with ETF file paths
    2. Define comparable companies in companies_to_value
    3. Run script to generate valuation reports

Author: DenisBeliaev
Date: September 2025
License: MIT

Dependencies:
    - yfinance
    - pandas
    - concurrent.futures
"""

from typing import Dict, List, Optional, Tuple, Any, Union
from functools import lru_cache
import warnings
import yfinance as yf
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress warnings from yfinance, which can sometimes be noisy
warnings.filterwarnings("ignore", category=FutureWarning)
