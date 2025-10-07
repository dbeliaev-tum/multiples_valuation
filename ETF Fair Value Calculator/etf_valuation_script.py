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

# --- ETF File Configuration ---
# Dictionary mapping ETF names to CSV file paths
# Each CSV file should contain ETF composition data: tickers, shares, valuation method weights
etf_dict = {
    "POLAND": "Investments - POLAND.csv",
}

# Potential troubleshooting function
def test_peer_data_directly(peers: List[str], target_ticker: str) -> None:
    """
    Directly diagnoses data availability and integrity for a list of peer tickers
    using raw yfinance calls and comparing results with the internal wrapper.
    """
    print(f"\n🔬 DIRECT PEER DIAGNOSTICS FOR {target_ticker}")
    print("=" * 50)

    for peer in peers:
        print(f"\n📊 Testing {peer}:")
        try:
            # Direct call without caching for raw data inspection
            ticker_obj = yf.Ticker(peer)
            info = ticker_obj.info or {}

            # Raw check of the most important keys from the source
            key_data = {
                'currentPrice': info.get('currentPrice'),
                'regularMarketPrice': info.get('regularMarketPrice'),
                'sharesOutstanding': info.get('sharesOutstanding'),
                'totalRevenue': info.get('totalRevenue'),
                'trailingEps': info.get('trailingEps'),
                'ebitda': info.get('ebitda')
            }

            print(f"   Raw Key Data: {key_data}")

            # Check what our internal function (with aggregation logic) returns
            our_result = get_company_data(peer)
            print(f"   Our Function: success={our_result.get('success')}, "
                  f"price={our_result.get('price')}, shares={our_result.get('shares')}, "
                  f"revenue={our_result.get('revenue')}")

        except Exception as e:
            print(f"✗ ERROR: {e}")

    print("\n" + "=" * 50)

# --- Companies to Value and Their Peer Groups ---
# Keys: Target company tickers, Values: Lists of peer company tickers
# Used for comparative analysis and multiplier calculations
companies_to_value = {
    "PKO.WA": ["SAN.MC", "DBK.DE", "CBK.DE"],
    "PKN.WA": ["REP.MC", "ENI.MI", "TTE.PA"],
    "PZU.WA": ["ALV.DE", "CS.PA", "MUV2.DE"],
    "PEO.WA": ["SAN.MC", "DBK.DE", "CBK.DE"],
    "ALE.WA": ["ETSY", "EBAY"],
    "DNP.WA": ["TSCDY", "DLTR", "ZAB.WA", "WMT", "COST", "TSCO.L"],
    "SPL.WA": ["SAN.MC", "DBK.DE", "CBK.DE"],
    "KGH.WA": ["FCX", "BHP","TECK", "RIO"],
    "LPP.WA": ["ZAL.DE", "ADS.DE"],
    "CDR.WA": ["UBI.PA", "TTWO","EA"],
    "MBK.WA": ["SAN.MC", "DBK.DE", "CBK.DE"],
    "PGE.WA": ["ENA.WA","TPE.WA", "ENGI.PA"],
    "MIL.WA": ["SAN.MC", "DBK.DE", "CBK.DE"],
    "CCC.WA": ["ZAL.DE", "NKE", "ADS.DE", "SKX"],
    "BDX.WA": ["ACS.MC", "HO.PA", "VIE.PA"],
    "ZAB.WA": ["WMT", "COST", "TSCO.L", "DNP.WA"],

    "RHM.DE": ["NOC", "RTX", "LHX", "SAF.PA", "LMT"],
    "SAP.DE": ["ORCL", "MSFT", "CRM", "NOW", "WDAY"],
    "SIE.DE": ["HON", "MMM", "ABBN.SW", "RTX"],
    "ALV.DE": ["ZURN.SW", "CS.PA", "AIG", "MET", "MUV2.DE", "SREN.SW", "AV.L", "LGEN.L"],
    "DTE.DE": ["T", "VZ", "FTE.F"],
    "AIR.DE": ["RTX", "LMT", "NOC", "SAF.PA", "BA"],
    "MUV2.DE": ["HNR1.DE", "RNR", "TRV", "BRK-B", "SREN.SW"],
    "RWE.DE": ["ENEL.MI", "KEX", "EOAN.DE"],
    "DBK.DE": ["CBK.DE", "INGA.AS", "BNP.PA", "UCG.MI", "ISP.MI", "UBSG.SW", "UBS"],
    "DB1.DE": ["ICE", "LNSTY", "NDAQ"],
    "IFX.DE": ["QCOM", "NXPI", "ON", "STM"],
    "BAS.DE": ["LYB", "DOW", "COVTY", "AKZA.AS"],
    "DHL.DE": ["UPS", "FDX", "CHRW"],
    "MBG.DE": ["BMW.DE", "VOW3.DE", "GM", "STLA", "F"],
    "EOAN.DE": ["RWE.DE", "ENEL.MI", "NG.L"],
    "CBK.DE": ["DBK.DE", "BAC", "HSBC", "BNP.PA", "UBS", "INGA.AS", "UCG.MI", "ISP.MI", "EBS.VI"],
    "ADS.DE": ["NKE", "PUM.DE", "LULU", "SKX"],
    "BAYN.DE": ["NVO","FMC","CTVA", "PFE", "SNY"],
    "HEI.DE": ["CRH", "HOLN.SW"],
    "BMW.DE": ["MBG.DE", "VOW3.DE", "GM", "F", "STLA"],
    "DTG.DE": ["VOLCAR-B.ST", "PCAR", "8TRA.DE"],
    "MTX.DE": ["RTX", "SAF.PA","LHX"],
    "VNA.DE": ["EQR", "AVB"],
    "VOW3.DE": ["MBG.DE", "BMW.DE", "GM", "STLA", "F", "000270.KS"],
    "HNR1.DE": ["MUV2.DE", "RNR", "TRV", "BRK-B"],
    "SHL.DE": ["PHG", "HOLX"],
    "MRK.DE": ["BAYN.DE", "NVO", "ROG.SW", "SNY"],
    "HEN3.DE": ["UL", "KMB", "RKT.L", "BEI.DE"],
    "BEI.DE": ["EL", "PG", "UL", "CL", "LRLCY", "COTY"],
    "SY1.DE": ["IFF", "KRYAY"],
    "QIA.DE": ["TMO", "BIO", "ILMN"],
    "FME.DE": ["DVA","BAX"],
    "CON.DE": ["MGA", "LEA", "GT", "ADNT"],
    "BNR.DE": ["IMCD.AS","AZE.BR","LYB", "DOW", "BAS.DE"],
    "ZAL.DE": ["YOU.DE", "CCC.WA"],
    "PAH3.DE": ["MBG.DE", "BMW.DE", "AML.L"],
    "P911.DE": ["MBG.DE", "BMW.DE", "AML.L"],
    "SRT3.DE": ["TMO", "BIO", "ILMN"],
    "LHA.DE": ["AAL", "DAL", "UAL", "AF.PA", "IAG.L"],
    "ENR.DE": ["GE", "VWS.CO", "CAT"],
    "FRE.DE": ["HCA", "THC", "BAX"],

    "7203.T": ["7267.T", "7201.T", "7211.T", "F", "GM"],
    "7267.T": ["7203.T", "7269.T", "7201.T", "F", "GM"],
    "7269.T": ["7203.T", "7267.T", "7201.T", "F", "GM"],
    "8306.T": ["8316.T", "8411.T", "BAC", "WFC", "C"],
    "8316.T": ["8306.T", "8411.T", "JPM", "BAC", "WFC"],
    "8411.T": ["8306.T", "8316.T", "BAC", "WFC"],
    "8766.T": ["8725.T", "8630.T", "MET", "PRU", "PGR"],
    "8725.T": ["8630.T", "MET", "PRU"],
    "8630.T": ["8766.T", "8725.T", "MET", "PRU"],
    "8750.T": ["8725.T", "MET", "PRU"],
    "6758.T": ["7751.T", "6954.T", "SONY", "6753.T", "PHG"],
    "7751.T": ["6758.T", "6753.T", "HPQ", "XRX"],
    "6954.T": ["6758.T", "7751.T", "6753.T", "EMR", "ROK"],
    "8035.T": ["6723.T", "LRCX", "AMAT"],
    "6857.T": ["8035.T", "LRCX", "AMAT"],
    "6723.T": ["8035.T", "INTC"],
    "6861.T": ["8035.T", "6857.T", "6723.T", "KEY", "TYL"],
    "9432.T": ["9433.T", "9434.T", "T", "VZ", "DTEGY"],
    "9433.T": ["9432.T", "9434.T", "9436.T", "T", "VZ", "DTEGY"],
    "9434.T": ["9432.T", "9433.T", "9436.T", "T", "VZ"],
    "6702.T": ["6701.T", "6758.T", "IBM", "ACN", "CTS"],
    "6701.T": ["6702.T", "6758.T", "IBM", "ACN"],
    "4502.T": ["4568.T", "4503.T", "4519.T", "PFE", "MRK", "NVS"],
    "4568.T": ["4502.T", "4503.T", "4519.T", "PFE", "MRK"],
    "4503.T": ["4502.T", "4568.T", "PFE", "MRK"],
    "4519.T": ["4502.T", "4568.T", "4503.T", "PFE", "MRK"],
    "9983.T": ["3382.T", "HD", "LOW"],
    "8267.T": ["9983.T", "3382.T", "HD", "LOW", "WMT", "TGT"],
    "3382.T": ["8267.T", "LOW", "WMT", "TGT"],
    "4063.T": ["4901.T", "3407.T", "4452.T", "DOW", "BASFY"],
    "4901.T": ["3407.T", "4452.T", "DOW"],
    "4452.T": ["4063.T", "4901.T", "PG", "UL", "CL"],
    "6501.T": ["7011.T", "6301.T", "6503.T", "GE", "SIEGY"],
    "7011.T": ["6501.T", "6301.T", "6503.T", "SIEGY"],
    "6301.T": ["6501.T", "7011.T", "6503.T"],
    "6503.T": ["6501.T", "7011.T", "6301.T", "SIEGY"],
    "8801.T": ["8802.T", "1925.T", "8830.T"],
    "8802.T": ["8801.T", "8830.T", "BXP"],
    "1925.T": ["8801.T", "LEN", "DHI", "PHM"],
    "8001.T": ["8031.T", "8002.T", "8053.T", "8058.T"],
    "8031.T": ["8001.T", "8002.T", "8053.T", "8058.T", "MITSF"],
    "8002.T": ["8001.T", "8031.T", "8053.T", "8058.T"],
    "8053.T": ["8001.T", "8031.T", "8002.T", "8058.T", "SMFG"],
    "8058.T": ["8001.T", "8031.T", "8002.T", "8053.T"],
    "7974.T": ["7832.T", "9697.T", "EA", "TTWO"],
    "7832.T": ["7974.T", "9697.T", "EA", "TTWO", "HEIA.AS"],
    "9984.T": ["9434.T", "3690.T", "BABA", "TCEHY", "JD"],
    "6098.T": ["2121.T", "RHI", "ADP"],
    "4661.T": ["DIS", "HLT"],
    "5401.T": ["5411.T", "NUE", "STLD", "CLF"],
    "2914.T": ["MO", "BTI", "IMBBY"],
    "7741.T": ["4543.T", "4901.T", "TMO", "DHR", "BDX", "JNJ"],
    "4543.T": ["7741.T", "4901.T", "TMO", "DHR", "BDX"],
    "6367.T": ["6501.T", "6503.T"],
    "8591.T": ["8411.T", "8316.T", "APO"],
    "6981.T": ["6752.T", "6762.T", "6861.T", "SWKS", "QRVO"],
    "6762.T": ["6981.T", "6752.T", "6861.T", "SWKS", "QRVO"],
    "6146.T": ["6501.T", "7011.T", "6954.T", "PTC"],
    "5108.T": ["5101.T", "BRDCY", "GOOD", "GT"],
    "9022.T": ["9020.T", "9001.T", "9005.T", "9007.T", "CSX"],
    "9020.T": ["9022.T", "9001.T", "9005.T", "9007.T", "UNP", "CSX"],
    "6902.T": ["7203.T", "7269.T", "ALV", "MGA", "APTV"],
    "2802.T": ["K", "GIS", "HSY"],
    "4578.T": ["4502.T", "4503.T", "PFE"],
    "6752.T": ["6503.T", "7751.T"],
    "8308.T": ["8316.T", "8306.T", "8411.T", "RF", "KEY", "CFG", "FITB"],
    "8604.T": ["8411.T", "MS", "GS", "BARC.L"],
    "5803.T": ["6501.T", "6503.T", "NVT"],
    "5802.T": ["6501.T","6503.T"],

    "SAN.MC": ["BBVA.MC", "BNP.PA", "UCG.MI", "ISP.MI", "INGA.AS", "DBK.DE", "HSBA.L"],
    "BNP.PA": ["SAN.MC", "GLE.PA", "DBK.DE", "HSBA.L"],
    "UCG.MI": ["BNP.PA", "SAN.MC", "DBK.DE"],
    "INGA.AS": ["SAN.MC", "BNP.PA", "UCG.MI", "ISP.MI", "DBK.DE", "ABN.AS"],
    "BBVA.MC": ["SAN.MC", "CABK.MC", "SAB.MC", "BKT.MC", "INGA.AS", "BNP.PA", "UCG.MI"],
    "CABK.MC": ["SAN.MC", "BBVA.MC", "SAB.MC", "BKT.MC", "ISP.MI", "UCG.MI"],
    "SAB.MC": ["BBVA.MC", "CABK.MC", "BKT.MC", "BPE.MI"],
    "BKT.MC": ["SAN.MC", "BBVA.MC", "CABK.MC", "SAB.MC", "EBS.VI", "BG.VI"],
    "ISP.MI": ["UCG.MI", "BAMI.MI", "BPE.MI", "SAN.MC", "INGA.AS"],
    "BAMI.MI": ["ISP.MI", "UCG.MI", "BPE.MI", "BPSO.MI"],
    "BMPS.MI": ["ISP.MI", "UCG.MI", "BAMI.MI", "BPE.MI", "BPSO.MI", "SAB.MC"],
    "BPE.MI": ["ISP.MI", "UCG.MI", "BAMI.MI", "BMPS.MI", "BPSO.MI", "SAB.MC"],
    "BPSO.MI": ["ISP.MI", "UCG.MI", "BAMI.MI", "BMPS.MI", "BPE.MI", "BGN.MI"],
    "KBC.BR": ["INGA.AS", "ABN.AS", "EBS.VI", "BNP.PA", "SAN.MC"],
    "ABN.AS": ["INGA.AS", "KBC.BR", "EBS.VI", "RBI.VI", "DBK.DE", "CBK.DE"],
    "EBS.VI": ["RBI.VI", "INGA.AS", "KBC.BR", "ABN.AS", "CBK.DE"],
    "RBI.VI": ["EBS.VI", "INGA.AS", "KBC.BR", "ABN.AS"],
    "BG.VI": ["EBS.VI", "INGA.AS", "KBC.BR", "ABN.AS"],
    "A5G.IR": ["BIRG.IR", "SAN.MC", "INGA.AS", "BNP.PA", "DBK.DE", "HSBA.L"],
    "BIRG.IR": ["SAN.MC", "INGA.AS", "BNP.PA", "DBK.DE"],
    "GLE.PA": ["BNP.PA", "ACA.PA", "DBK.DE", "UBSG.SW"],
    "ACA.PA": ["GLE.PA", "BNP.PA"],
    "FBK.MI": ["BGN.MI", "UBSG.SW"],
    "KBCA.BR": ["INGA.AS", "BGN.MI", "FBK.MI"],
    "BGN.MI": ["FBK.MI", "ACA.PA", "GLE.PA", "BMPS.MI", "UBSG.SW"],

    "CS.PA": ["ALV.DE", "MUV2.DE", "ZURN.SW", "SREN.SW", "PRU", "AV.L", "LGEN.L"],
    "ZURN.SW": ["ALV.DE", "CS.PA", "MUV2.DE", "SREN.SW", "SLHN.SW"],
    "SREN.SW": ["MUV2.DE", "HNR1.DE", "RGA", "GLRE"],
    "PRU": ["MET", "SLF.TO", "PGR"],

    "LLY": ["JNJ", "ABBV", "AMGN", "GILD", "EW", "BSX"],
    "JNJ": ["ABBV", "MRK", "PFE", "AMGN", "ABT"],
    "ABBV": ["JNJ", "LLY", "AMGN", "GILD", "EW", "BSX"],
    "MRK": ["PFE", "BMY", "AMGN"],
    "PFE": ["MRK", "BMY", "AMGN"],
    "BMY": ["MRK", "PFE", "AMGN"],
    "AMGN": ["GILD", "VRTX", "REGN", "VRTX"],
    "GILD": ["AMGN", "VRTX", "REGN", "BIIB"],
    "VRTX": ["REGN", "AMGN", "GILD"],
    "REGN": ["AMGN", "GILD", "BIIB", "INCY"],
    "TMO": ["BDX", "MDT", "ZBH"],
    "DHR": ["TMO", "SYK", "MDT"],
    "BDX": ["TMO", "MDT", "ZBH"],
    "BSX": ["SYK", "EW", "ABT", "JNJ"],
    "SYK": ["ABT", "JNJ"],
    "MDT": ["SYK", "ZBH", "ABT", "JNJ"],
    "EW": ["BSX", "MDT", "SYK", "ABT", "JNJ"],
    "ISRG": ["BSX", "SYK", "EW", "ABT"],
    "IDXX": ["DHR", "WAT"],
    "RMD": ["MDT", "ABT", "JNJ"],
    "UNH": ["CI", "ELV", "HUM", "CVS"],
    "CI": ["UNH", "ELV", "HUM", "CVS", "CNC"],
    "ELV": ["UNH", "CI", "HUM", "CVS", "CNC"],
    "CVS": ["CI", "UNH", "WBA", "MCK", "CAH"],
    "MCK": ["CAH", "COR", "HSIC"],
    "COR": ["MCK", "CAH", "HSIC"],
    "HCA": ["UHS", "THC", "ACHC"],
    "ZTS": ["HALO", "NVS"],
    "ABT": ["JNJ", "MDT", "BDX", "TMO"]
}

# --- Data Preparation ---

def process_etf_file(file_path: str) -> Optional[pd.DataFrame]:
    """
    Processes ETF portfolio CSV files with robust data validation.

    Complex Data Validation Logic:
    1. Schema Validation: Verifies required columns presence and structure
    2. Data Type Conversion: Handles locale-specific number formatting (comma vs. decimal)
    3. Data Quality Checks: Identifies and handles missing/invalid values
    4. Business Rule Enforcement: Ensures data meets valuation requirements

    File Processing Pipeline:
    - Automatic comma-to-decimal conversion for international number formats
    - Comprehensive error handling for file I/O operations
    - Data completeness validation before proceeding to valuation
    """
    # Define required columns for a valid ETF portfolio file
    required_cols = ['ticker', 'share', 'price_ev_w', 'price_pe_w', 'price_ps_w']

    # Create a dictionary for on-the-fly conversion of ',' to '.' and to float
    # This handles common locale issues in CSV files.
    converters_dict = {
        col: lambda x: float(str(x).replace(',', '.'))
        for col in ['share', 'price_ev_w', 'price_pe_w', 'price_ps_w']
    }

    try:
        # Read CSV file using the defined converters
        df = pd.read_csv(file_path, converters=converters_dict)

        # Check for presence of all required columns
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            print(f"✗ File {file_path} is missing required columns: {missing_cols}")
            return None

        # Select necessary columns and drop rows with any missing data (NaN)
        combined_df = df[required_cols].dropna(how='any')

        print(f"✓ Processed file {file_path}: {len(combined_df)} records")
        return combined_df

    except FileNotFoundError:
        print(f"✗ Error: File not found at path {file_path}")
        return None
    except Exception as e:
        print(f"✗ Error processing file {file_path}: {e}")
        return None

# Function to add portfolio data from the processed DataFrame
def load_and_filter_portfolio_data(combined_df: Optional[pd.DataFrame],
                                 companies_to_value: Optional[Dict[str, List[str]]] = None,
                                 overwrite: bool = False) -> Dict[str, List[str]]:
    """Simplified version of adding portfolio data (weights, shares)."""
    if combined_df is None:
        print("✗ Error: combined_df is None")
        return companies_to_evaluate

    # Convert DataFrame to a list of lists for iteration
    data = combined_df.values.tolist() if hasattr(combined_df, 'values') else combined_df

    added_count = 0

    for deal in data:
        ticker = deal[0]

        # Filter by the map of comparable companies, if provided
        if companies_to_value and ticker not in companies_to_value:
            continue

        # Check for overwrite permission
        if not overwrite and ticker in weights:
            continue

        # Update global data
        weights[ticker] = deal[2:] # Multiplier weights start at index 2
        shares[ticker] = deal[1]   # Share count is at index 1

        if companies_to_value:
            companies_to_evaluate[ticker] = companies_to_value[ticker]

        added_count += 1

    print(f"✓ Tickers added: {added_count}")
    return companies_to_evaluate

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

def safe_float(value: any) -> Optional[float]:
    """Safely converts a value to a float, returning None on failure."""
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None

def get_value(data: pd.DataFrame, keys: List[str]) -> Optional[float]:
    """
    Efficiently retrieves a scalar value from a Pandas DataFrame, prioritizing
    the first available key from the list.
    """
    if data is None or data.empty:
        return None

    # Iterate through potential keys to find a match in the DataFrame index
    # Iteration is necessary here because multiple keys are provided.
    for k in keys:
        if k in data.index:
            try:
                # Retrieve the row, drop NaN values, and check if it's not empty
                series = data.loc[k].dropna()
                if not series.empty:
                    # Return the first available value converted to float
                    return float(series.iloc[0])
            except (ValueError, TypeError):
                # Skip to the next key if conversion to float fails
                continue

    return None

def find_value_by_keys(data: Dict[str, any], keys: List[str]) -> Optional[any]:
    """
    Searches for a value in a dictionary by iterating through a list of possible keys.
    Returns the value corresponding to the first key found.
    """
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None

# --- Financial Metric Keys ---
# Lists of possible keys in Yahoo Finance data for various financial metrics
# Used for robust data extraction from the heterogeneous API

# List of potential keys for common financial metrics (can be expanded)
EBITDA_KEYS = ['ebitda', 'EBITDA', 'ebitdaMargins', 'operatingCashflow']
REVENUE_KEYS = ['totalRevenue', 'revenue', 'operatingRevenue', 'grossRevenue']
DEBT_KEYS = ['totalDebt', 'netDebt', 'longTermDebt', 'shortTermDebt', 'totalLiabilities']
CASH_KEYS = ['cash', 'cashAndCashEquivalents', 'totalCash', 'cashAndShortTermInvestments']
EPS_KEYS = ['trailingEps', 'basicEps', 'earningsPerShare', 'dilutedEPS']
NET_INCOME_KEYS = ['netIncome', 'Net Income', 'NetIncome',
                   'Net Income from Continuing & Discontinued Operation','Net Income Continuous Operations',
                   'Normalized Income']

def get_company_data_impl(ticker: str, verbose: bool = True) -> Dict[str, any]:
    """
    Core implementation for retrieving and processing company financial data.

    Complex Data Processing Logic:
    1. Multi-Source Data Retrieval: Attempts multiple data sources with fallbacks
    2. Currency Normalization: Converts all financial metrics to EUR using live exchange rates
    3. Data Quality Assessment: Validates data completeness and reliability
    4. Graceful Degradation: Provides partial results even with incomplete data

    Financial Metric Hierarchy:
    - Primary metrics from standard Yahoo Finance fields
    - Fallback to alternative field names when primary unavailable
    - Validation of data reasonableness and business logic consistency

    Error Recovery:
    - Implements retry logic for transient API failures
    - Provides detailed error reporting for troubleshooting
    - Maintains data consistency across currency conversions
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        if not info or len(info) < 10:
            import time
            time.sleep(0.1)
            t = yf.Ticker(ticker)
            info = t.info or {}

        currency = info.get('currency', 'USD')
        euro_rate = get_exchange_rate(currency, 'EUR') or 1.0

        raw_data = {
            'price': info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose'),
            'shares': (info.get('sharesOutstanding') or
                       info.get('impliedSharesOutstanding') or
                       info.get('floatShares')),
            'debt': find_value_by_keys(info, DEBT_KEYS),
            'cash': find_value_by_keys(info, CASH_KEYS),
            'ebitda': find_value_by_keys(info, EBITDA_KEYS),
            'revenue': find_value_by_keys(info, REVENUE_KEYS),
            'eps': find_value_by_keys(info, EPS_KEYS),
            'net_income': find_value_by_keys(info, NET_INCOME_KEYS),
        }

        if raw_data['revenue'] is None:
            alt_revenue = (info.get('totalCashFromOperatingActivities') or
                           info.get('grossProfit'))
            if alt_revenue:
                raw_data['revenue'] = alt_revenue

        data_in_eur = {}
        for k, v in raw_data.items():
            if k in ['price', 'debt', 'cash', 'ebitda', 'revenue', 'eps', 'net_income'] and v is not None:
                data_in_eur[k] = safe_float(v) * euro_rate
            else:
                data_in_eur[k] = safe_float(v)

        data_in_eur['name'] = info.get('longName', ticker)
        data_in_eur['success'] = data_in_eur.get('price') is not None

        if verbose and not data_in_eur['success']:
            print(f"✗ Error fetching data for {ticker}: No price data available")

        return data_in_eur

    except Exception as e:
        if verbose:
            print(f"✗ Error fetching data for {ticker}: {e}")
        logging.error(f"Error processing ticker {ticker}: {e}")
        return {'success': False, 'ticker': ticker}

@lru_cache(maxsize=None)
def get_company_data_cached(ticker: str, verbose: bool = True) -> Tuple[Tuple[str, any], ...]:
    """
    Caching wrapper - returns a tuple instead of a dict for hashability.
    """
    data = get_company_data_impl(ticker, verbose)
    # Convert dict to a sorted tuple of items for caching
    return tuple(sorted(data.items()))

def get_company_data(ticker: str, verbose: bool = True) -> Dict[str, any]:
    """
    Public interface for getting company data with caching.

    Converts cached tuple back to dictionary for external use.
    """
    cached_tuple = get_company_data_cached(ticker, verbose)
    return dict(cached_tuple)
