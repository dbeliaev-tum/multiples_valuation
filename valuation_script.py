"""
Market Multiples Valuation Script

This script performs relative valuation of companies by comparing them to peer groups
using key market multiples (EV/EBITDA, P/E, P/S). It fetches financial data from
Yahoo Finance, calculates average peer multiples, and applies these to target
companies to estimate fair value.

Author: DenisBeliaev
Date: September 2025
License: MIT
"""

from typing import Dict, List, Optional, Tuple
from functools import lru_cache
import warnings
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Suppress warnings from yfinance, which can sometimes be noisy
warnings.filterwarnings("ignore", category=FutureWarning)

# --- Companies to Value and Their Peer Groups ---
# Keys: Target company tickers, Values: Lists of peer company tickers
# Used for comparative analysis and multiplier calculations
companies_to_value = {
    "KO": ["PEP", "KDP", "MNST"],
    "AIR.PA": ["RTX", "LMT", "SAF.PA","NOC", "BA"],
    "RHM.DE": ["NOC", "RTX", "LHX", "SAF.PA", "LMT"],
    "LMT": ["NOC", "RTX", "GD", "LHX"],
    "BTI": ["PM", "MO"],
    "SID.F": ["IBN", "BBD", "ITUB", "HDB"],
    "VIB3.F": ["FSKRS.HE", "GEBN.SW", "FSKRS.HE", "MAS","MHK", "5938.T"],
    "WMT": ["TGT", "COST", "BJ"],
    "COST": ["WMT", "TGT", "BJ", "LOW"],
    "NVDA": ["AVGO", "AMD", "ARM", "INTC", "QCOM", "ASML.AS", "MU"],
    "BRK-B": ["BLK","MKL", "L", "BN", "JEF"],
    "MSFT": ["ORCL", "SAP.DE", "ADBE", "NOW"],
    "MCD": ["SBUX", "YUM", "CMG", "QSR", "DPZ"],
    "HDB": ["IBN", "BBD", "ITUB", "SID.F"],
    "SBUX": ["MCD", "JDEP.AS","CMG", "QSR"],
    "AAPL": ["MSFT", "GOOGL"],
    "NESN.SW": ["UL", "RBGLY", "DANOY", "MDLZ"],

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

# Dictionary mapping ETF names to CSV file paths
etf_dict = {
    "POLAND": "Investments - POLAND.csv"
}

# Dictionary mapping stock portfolio names to CSV file paths
stocks_dict = {
    "Sample": "sample_deals_yahoo.csv"
    # "Test": "test_deals_yahoo.csv"
}

# --- Data Preparation ---

def prepare_csv_data(file_path: str,
                     companies_to_value: Optional[Dict[str, List[str]]] = None) -> Tuple[Dict[str, Tuple[float, float, float]], Dict[str, List[str]]]:
    """
        Prepares CSV file data for valuation processing.

        Complex Business Logic:
        - Handles European number formatting (comma as decimal separator)
        - Validates required columns and data completeness
        - Filters companies based on valuation dictionary
        - Converts numeric columns with robust error handling

        Args:
            file_path: Path to the CSV file
            companies_to_value: Optional dictionary of companies to filter for evaluation

        Returns:
            Tuple containing:
            - weights: Dictionary mapping tickers to valuation method weights
            - companies_to_evaluate: Filtered dictionary of companies for evaluation

        Raises:
            FileNotFoundError: When specified file doesn't exist
            ValueError: When required columns are missing or data conversion fails
        """

    # Required columns for processing
    required_columns = ['ticker', 'price_ev_w', 'price_pe_w', 'price_ps_w']

    try:
        # Read CSV with European number format handling
        df = pd.read_csv(
            file_path,
            sep=',',
            decimal=',',
            thousands='.',
            quotechar='"',
            encoding='utf-8'
        ).dropna(how='all')  # Remove completely empty rows

    except FileNotFoundError:
        raise FileNotFoundError(f"✗ File '{file_path}' not found.")

    # Check for required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"✗ Missing required columns: {', '.join(missing_cols)}")

    # Select required columns and remove rows with missing values
    data_df = df[required_columns].dropna()

    if data_df.empty:
        # Show available data statistics
        stats = {col: df[col].notna().sum() for col in required_columns}
        raise ValueError(f"✗ No data available after filtering. Available data: {stats}")

    # Convert numeric columns to float (handle comma as decimal separator)
    numeric_cols = ['price_ev_w', 'price_pe_w', 'price_ps_w']
    for col in numeric_cols:
        try:
            data_df[col] = data_df[col].astype(str).str.replace(',', '.').astype(float)
        except ValueError as e:
            raise ValueError(f"✗ Error converting column '{col}': {e}")

    # Create weights dictionary
    weights: Dict[str, Tuple[float, float, float]] = {
        row['ticker']: (row['price_ev_w'], row['price_pe_w'], row['price_ps_w'])
        for _, row in data_df.iterrows()
    }

    # Filter companies for evaluation
    companies_to_evaluate = {}
    if companies_to_value:
        for ticker in data_df['ticker'].unique():
            if ticker in companies_to_value:
                companies_to_evaluate[ticker] = companies_to_value[ticker]
            else:
                print(f"⚠ Ticker {ticker} not found in valuation database.")

    print(f"✓ Successfully processed {len(weights)} records from file '{file_path}'")
    return weights, companies_to_evaluate

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

def find_value_by_keys(data: Dict[str, any], keys: List[str]) -> Optional[any]:
    """
    Searches for a value in a dictionary using multiple possible keys.

    Returns the value corresponding to the first key found in the list.
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

    Complex Business Logic:
    - Multi-source data retrieval with fallback mechanisms
    - Automatic currency conversion to EUR using live exchange rates
    - Robust error handling for API failures and data inconsistencies
    - Comprehensive data validation and quality assessment

    Financial Metric Processing:
    - Fetches key metrics: price, shares, debt, cash, EBITDA, revenue, EPS, net income
    - Handles alternative data sources when primary metrics are unavailable
    - Converts all financial values to EUR for consistent comparison
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

def get_multiple_companies_data(tickers: List[str]) -> Dict[str, dict]:
    """
    Sequential data retrieval for multiple companies with automatic caching.

    Includes strategic delays to respect API rate limits.
    """
    results = {}
    for ticker in tickers:
        results[ticker] = get_company_data(ticker)
        import time
        time.sleep(0.1)
    return results

def calculate_peer_multipliers(peers: List[str]) -> Dict[str, any]:
    """
    Calculates average valuation multiples for peer companies.

    Complex Business Logic:
    1. Collects financial data for all peer companies
    2. Calculates Enterprise Value (EV) = Market Cap + Debt - Cash
    3. Computes three key valuation multiples:
       - EV/EBITDA: Enterprise Value to EBITDA (debt-inclusive valuation)
       - P/E: Price to Earnings per Share (profitability-based valuation)
       - P/S: Price to Sales per Share (revenue-based valuation)
    4. Filters outliers and invalid values using reasonable bounds
    5. Calculates mean values across successful peers

    Outlier Filtering:
    - EV/EBITDA: excludes values > 50 (atypically high)
    - P/E: excludes values > 100 (atypically high)
    - P/S: excludes values > 40 (atypically high)

    Data Quality Assurance:
    - Only includes companies with successful data retrieval
    - Handles missing financial metrics gracefully
    - Provides detailed reporting of failed companies

    Returns:
        Dictionary with average multipliers and metadata including:
        - successful_peers: List of companies used in calculation
        - peers_count: Number of successful peers
        - individual_multipliers: Detailed results per company
    """
    # Load data for all peers using the optimized function
    all_data_result = get_multiple_companies_data(peers)
    df = pd.DataFrame.from_dict(all_data_result, orient='index')

    # Filter successful data loads
    successful = df[df['success']].copy()

    if successful.empty:
        print("⚠ No successful data loads for any peer companies")
        return {
            'ev_ebitda': None,
            'p_e': None,
            'p_s': None,
            'successful_peers': [],
            'peers_count': 0,
            'individual_multipliers': {}
        }

    df = successful

    # Fill missing debt and cash with zeros for EV calculation
    df['debt'] = df['debt'].fillna(0)
    df['cash'] = df['cash'].fillna(0)

    # Calculate Enterprise Value (EV)
    df['ev'] = df['price'] * df['shares'] + df['debt'] - df['cash']

    # Calculate Multipliers
    # EV/EBITDA
    df['ev_ebitda'] = None
    mask_ebitda = (df['ebitda'].notna()) & (df['ebitda'] > 0)
    if mask_ebitda.sum() > 0:
        df.loc[mask_ebitda, 'ev_ebitda'] = df.loc[mask_ebitda, 'ev'] / df.loc[mask_ebitda, 'ebitda']

    # P/E
    df['p_e'] = None
    mask_eps = (df['eps'].notna()) & (df['eps'] > 0)
    if mask_eps.sum() > 0:
        df.loc[mask_eps, 'p_e'] = df.loc[mask_eps, 'price'] / df.loc[mask_eps, 'eps']

    # P/S (corrected formula: Price / (Revenue / Shares))
    df['sales_per_share'] = df['revenue'] / df['shares']
    df['p_s'] = None
    mask_sales = (df['sales_per_share'].notna()) & (df['sales_per_share'] > 0)
    if mask_sales.sum() > 0:
        df.loc[mask_sales, 'p_s'] = df.loc[mask_sales, 'price'] / df.loc[mask_sales, 'sales_per_share']

    # Prepare individual multipliers for output
    individual_multipliers = {}
    successful_peers = []

    for idx, row in df.iterrows():
        company_name = row.get('name', idx)
        multipliers = {}

        if pd.notna(row.get('ev_ebitda')) and 0 < row['ev_ebitda'] < 50:
            multipliers['ev_ebitda'] = row['ev_ebitda']

        if pd.notna(row.get('p_e')) and 0 < row['p_e'] < 100:
            multipliers['p_e'] = row['p_e']

        if pd.notna(row.get('p_s')) and 0 < row['p_s'] < 40:
            multipliers['p_s'] = row['p_s']

        if multipliers:  # Only add if at least one multiplier is calculable
            individual_multipliers[company_name] = multipliers
            successful_peers.append(company_name)

            # Print individual company results
            print(f"✔ Processed: {company_name} ({idx})")
            multipliers_str = []
            if 'ev_ebitda' in multipliers:
                multipliers_str.append(f"EV/EBITDA: {multipliers['ev_ebitda']:.2f}")
            if 'p_e' in multipliers:
                multipliers_str.append(f"P/E: {multipliers['p_e']:.2f}")
            if 'p_s' in multipliers:
                multipliers_str.append(f"P/S: {multipliers['p_s']:.2f}")

            if multipliers_str:
                print(f"   Multipliers: {', '.join(multipliers_str)}")

    # Final calculation (Mean/Median) with flexible limits
    results = {}

    for multiplier, col, max_val in [
        ('ev_ebitda', 'ev_ebitda', 50),
        ('p_e', 'p_e', 100),
        ('p_s', 'p_s', 40)
    ]:
        # Filter for positive, non-null values within reasonable limits
        all_values = df[(df[col] > 0) & (df[col] < max_val)][col].dropna()

        if all_values.empty:
            results[multiplier] = None
            continue

        # Use mean of filtered values
        results[multiplier] = all_values.mean()

    # Add metadata for compatibility
    results.update({
        'successful_peers': successful_peers,
        'peers_count': len(successful_peers),
        'individual_multipliers': individual_multipliers
    })

    # Print summary
    # print("\n--- AVERAGE MULTIPLES ---")
    # print(f"Average P/E: {results['p_e']:.2f}" if results['p_e'] else "N/A (No peer data)")
    # print(f"Average P/S: {results['p_s']:.2f}" if results['p_s'] else "N/A (No peer data)")
    # print(f"Average EV/EBITDA: {results['ev_ebitda']:.2f}" if results['ev_ebitda'] else "N/A (No peer data)")
    # print("-------------------------")

    # Report failed companies
    failed_peers = set(peers) - set(df.index)
    if failed_peers:
        print(f"⚠ The following companies could not be used (no data): {', '.join(failed_peers)}")

    return results

def valuate_company(ticker: str, multipliers: Dict[str, any],
                    weights: Dict[str, Tuple[float, float, float]]) -> Dict[str, any]:
    """
    Values a company using comparable company analysis methodology.

    Complex Business Logic:
    - Weight Redistribution: Automatically redistributes weights when valuation methods are unavailable
    - Multi-Method Valuation: Applies EV/EBITDA, P/E, and P/S methods with configurable weights
    - Data Validation: Comprehensive checks for data availability and quality

    Weight Redistribution Example:
    Initial weights: [0.4, 0.3, 0.3] for [EV/EBITDA, P/E, P/S]
    If P/E data unavailable → New weights: [0.4/(0.4+0.3), 0, 0.3/(0.4+0.3)] = [0.57, 0, 0.43]

    Valuation Methods:
    - EV/EBITDA: Fair Price = (Peer EV/EBITDA × Company EBITDA - Debt + Cash) / Shares
    - P/E: Fair Price = Peer P/E × Company EPS
    - P/S: Fair Price = Peer P/S × (Company Revenue / Shares)
    """
    data = get_company_data(ticker)

    # Check for base data failure
    if not data or not data.get('success'):
        return {
            'success': False,
            'error': 'Failed to fetch target company data',
            'ticker': ticker
        }

    # Get weights for this ticker
    w_ev, w_pe, w_ps = weights.get(ticker, (0.33, 0.33, 0.33))

    # Method availability checks
    # EV/EBITDA Check
    ev_m = multipliers.get('ev_ebitda')
    ev_ebitda = data.get('ebitda')
    ev_shares = data.get('shares')
    ev_available = (
            ev_m is not None and ev_ebitda is not None and ev_shares is not None
            and ev_ebitda != 0 and ev_shares != 0
    )

    # P/E Check
    pe_m = multipliers.get('p_e')
    pe_eps = data.get('eps')
    pe_available = (pe_m is not None and pe_eps is not None and pe_eps != 0)

    # P/S Check
    ps_m = multipliers.get('p_s')
    ps_revenue = data.get('revenue')
    ps_shares = data.get('shares')
    ps_available = (
            ps_m is not None and ps_revenue is not None and ps_shares is not None
            and ps_shares != 0 and ps_revenue != 0
    )

    # Weight redistribution logic
    available_weights = {}
    if ev_available: available_weights['ev'] = w_ev
    if pe_available: available_weights['pe'] = w_pe
    if ps_available: available_weights['ps'] = w_ps

    if not available_weights:
        return {
            'success': False,
            'error': 'Not enough data to perform valuation',
            'ticker': ticker
        }

    total_initial_weight = sum(available_weights.values())
    if total_initial_weight == 0:
        return {
            'success': False,
            'error': 'All available methods have zero weight',
            'ticker': ticker
        }

    # Normalize weights: distribute unavailable weight to available methods
    normalized_weights = {key: value / total_initial_weight for key, value in available_weights.items()}

    # Valuation calculations
    valuation_options = []
    calculations = {}

    # EV/EBITDA (Debt and Cash use default 0 if missing)
    if 'ev' in normalized_weights:
        ebitda_val = data['ebitda']
        ev = multipliers['ev_ebitda'] * ebitda_val
        price_ev = (ev - data.get('debt', 0) + data.get('cash', 0)) / data['shares']
        if price_ev > 0:
            valuation_options.append((price_ev, normalized_weights['ev']))
            calculations['ev_ebitda'] = price_ev

    # P/E
    if 'pe' in normalized_weights:
        price_pe = multipliers['p_e'] * data['eps']
        if price_pe > 0:
            valuation_options.append((price_pe, normalized_weights['pe']))
            calculations['p_e'] = price_pe

    # P/S
    if 'ps' in normalized_weights:
        sales_per_share = data['revenue'] / data['shares']
        price_ps = multipliers['p_s'] * sales_per_share
        if price_ps > 0:
            valuation_options.append((price_ps, normalized_weights['ps']))
            calculations['p_s'] = price_ps

    if not valuation_options:
        return {
            'success': False,
            'error': 'No valid positive valuation results',
            'ticker': ticker
        }

    # Calculate Weighted Average
    final_price = sum(p * w for p, w in valuation_options) / sum(w for p, w in valuation_options)
    final_price_rounded = round(final_price, 2)

    # Calculate premium/discount
    current_price = data.get('price')
    premium_discount = None
    if current_price and current_price > 0:
        premium_discount = round((final_price_rounded / current_price - 1) * 100, 1)

    # Print method availability info
    print(f"\n--- VALUATION METHODS USED FOR {ticker} ---")
    if 'ev' in normalized_weights:
        print(f"✓ EV/EBITDA method: weight {normalized_weights['ev']:.2f}")
    else:
        print("✗ EV/EBITDA method: insufficient data")

    if 'pe' in normalized_weights:
        print(f"✓ P/E method: weight {normalized_weights['pe']:.2f}")
    else:
        print("✗ P/E method: insufficient data")

    if 'ps' in normalized_weights:
        print(f"✓ P/S method: weight {normalized_weights['ps']:.2f}")
    else:
        print("✗ P/S method: insufficient data")
    print("----------------------------------------")

    return {
        'success': True,
        'ticker': ticker,
        'company_name': data.get('name', ticker),
        'current_price': current_price,
        'fair_price': final_price_rounded,
        'premium_discount': premium_discount,
        'calculations': calculations,
        'peers_used': multipliers.get('successful_peers', []),
        'peers_count': multipliers.get('peers_count', 0),
        'individual_multipliers': multipliers.get('individual_multipliers', {}),
        'weights_used': normalized_weights
    }

# --- ETF Valuation Extension ---

def process_etf_file(file_path):
    """
    Optimized function for processing the investment portfolio file (CSV).
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

        print(f"✔ Processed file {file_path}: {len(combined_df)} records")
        return combined_df

    except FileNotFoundError:
        print(f"✗ Error: File not found at path {file_path}")
        return None
    except Exception as e:
        print(f"✗ Error processing file {file_path}: {e}")
        return None

# Function to add portfolio data from the processed DataFrame
def load_and_filter_portfolio_data(combined_df, companies_to_value=None, overwrite=False):
    """Simplified version of adding portfolio data (weights, shares)."""
    global weights, shares, companies_to_evaluate

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

def calculate_etf_value_core(comparable_companies: Dict[str, List[str]],
                             weights: Dict,
                             shares_dict: Dict[str, float]) -> Dict:
    """
    Core logic for calculating ETF fair value using bottom-up valuation.

    Complex Multi-Stage Business Logic:
    1. Parallel Data Retrieval: Fetches financial data for all ETF constituents concurrently
    2. Parallel Multiplier Calculation: Computes peer-based multiples for each company in parallel
    3. Data Validation Pipeline: Comprehensive checks for:
       - Successful data retrieval (price, shares, financial metrics)
       - Valid share counts and positive prices
       - Successful multiplier calculations
       - Positive fair value results
    4. Result Aggregation: Sums individual company valuations to derive ETF-level values
    5. Premium/Discount Analysis: Compares aggregate current vs. fair values

    Error Handling & Resilience:
    - Continues processing even if individual companies fail
    - Provides detailed diagnostics for failed valuations
    - Maintains data integrity through rigorous validation
    - Offers transparent reporting of success/failure rates

    Performance Optimization:
    - Uses ThreadPoolExecutor for concurrent API calls
    - Implements LRU caching to minimize redundant requests
    - Includes strategic delays to respect API rate limits

    Returns:
        ETF valuation results with comprehensive metadata:
        - current_price_etf: Total current market value
        - fair_value_etf: Total calculated fair value
        - premium_discount_pct: Aggregate premium/discount percentage
        - companies_valuated: Count of successfully valued constituents
        - failed_companies: Detailed list of valuation failures
    """

    # Filter tickers to include only those present in the shares dictionary
    tickers = [t for t in comparable_companies if t in shares_dict]
    if not tickers:
        return {'success': False, 'error': 'No valid tickers with shared count'}

    print(f"\n--- 🔍 Starting ETF analysis with {len(tickers)} tickers ---")

    # 1. Parallel Data Retrieval
    companies_data = get_multiple_companies_data(tickers)

    successful_data = [t for t in tickers if companies_data.get(t, {}).get('success')]
    failed_data = [t for t in tickers if t not in successful_data]
    if failed_data:
        print(f"✗ Failed to get base data for: {failed_data}")

    # 2. Parallel Multiplier Calculation
    multipliers_data = {}
    if tickers:
        with ThreadPoolExecutor(max_workers=10) as executor:
            multipliers_list = list(executor.map(
                calculate_peer_multipliers,
                [comparable_companies[t] for t in tickers]
            ))
        multipliers_data = dict(zip(tickers, multipliers_list))

    # 3. Final Valuation and Aggregation
    total_current, total_fair, successful = 0.0, 0.0, 0
    failed_companies = []

    for ticker in tickers:
        data = companies_data.get(ticker, {})

        # Check 1: Ensure base data was retrieved successfully
        if not data.get('success'):
            failed_companies.append(f"{ticker}: no base data")
            continue

        # Check 2-3: Price and Share Count validity
        price = data.get('price')
        share_count = shares_dict.get(ticker)

        if not price or price <= 0:
            failed_companies.append(f"{ticker}: invalid price ({price})")
            continue
        if not isinstance(share_count, (int, float)) or share_count <= 0:
            failed_companies.append(f"{ticker}: invalid share count ({share_count})")
            continue

        current_value = price * share_count

        # Check 4: Fair Price Calculation Readiness
        if ticker not in multipliers_data or not multipliers_data[ticker]:
            failed_companies.append(f"{ticker}: multipliers not calculated")
            continue

        # Call valuate_company and extract the result
        valuation_result = valuate_company(ticker, multipliers_data[ticker], weights)

        # Check if valuation was successful
        if not valuation_result.get('success'):
            failed_companies.append(f"{ticker}: valuation failed - {valuation_result.get('error', 'unknown error')}")
            continue

        # Extract fair_price from the result dictionary
        fair_price = valuation_result.get('fair_price')

        if not fair_price or fair_price <= 0:
            failed_companies.append(f"{ticker}: failed to calculate fair_price (result {fair_price})")
            continue

        # Aggregate successful valuations
        total_current += current_value
        total_fair += fair_price * share_count
        successful += 1
        print(f"✔ Successfully valued {ticker}: price {price:.2f} → fair {fair_price:.2f}")

    # Output detailed error information
    if failed_companies:
        print("\n🔍 Problematic companies:")
        for error in failed_companies:
            print(f"   {error}")

    if successful == 0 or total_current == 0:
        return {'success': False, 'error': f'Valuation failed. Successfully valued 0 out of {len(tickers)}.'}

    # Calculate Premium/Discount
    premium_discount = ((total_fair - total_current) / total_current) * 100

    return {
        'success': True,
        'current_price_etf': total_current,
        'fair_value_etf': total_fair,
        'premium_discount_pct': premium_discount,
        'companies_valuated': successful,
        'total_companies': len(tickers),
        'failed_companies': failed_companies
    }

def calculate_etf_fair_value_wrapper(file_path: str,
                                     comparable_map: Dict[str, List[str]]) -> Dict:
    """
    A unified wrapper function for calculating the fair value of an ETF.
    """
    # 1. Load and process data from the file
    combined_df = process_etf_file(file_path)

    if combined_df is None:
        print(f"✗ Failed to load data from file: {file_path}")
        return {'success': False, 'error': f"Failed to load data from {file_path}"}

    # 2. Create LOCAL copies of weights and shares for this ETF only
    local_weights = {}
    local_shares = {}
    local_companies = {}

    # Parse the DataFrame manually without using global variables
    data = combined_df.values.tolist()
    added_count = 0

    for deal in data:
        ticker = deal[0]

        # Filter by comparable_map
        if comparable_map and ticker not in comparable_map:
            continue

        # Store in LOCAL dictionaries
        local_weights[ticker] = deal[2:]  # Multiplier weights
        local_shares[ticker] = deal[1]  # Share count

        if comparable_map:
            local_companies[ticker] = comparable_map[ticker]

        added_count += 1

    print(f"✓ Tickers added: {added_count}")

    # 3. Call the main valuation core logic with LOCAL data
    result = calculate_etf_value_core(
        local_companies,
        local_weights,
        local_shares
    )

    # 4. Output results
    if result['success']:
        print("\n--- 💰 FINAL ETF VALUATION RESULT ---")
        print(f"✔ ETF Current Value: €{result['current_price_etf']:,.2f}")
        print(f"✔ ETF Fair Value: €{result['fair_value_etf']:,.2f}")
        print(f"✔ Premium/Discount: {result['premium_discount_pct']:+.2f}%")
        print(f"✔ Successfully Valued: {result['companies_valuated']}/{result['total_companies']} companies")
    else:
        print(f"✗ Error: {result.get('error')}")

    return result

# -------------------------------

def run_valuation(stocks_dict: Dict[str, str],
                  companies_to_value: Optional[Dict[str, List[str]]] = None) -> List[Dict[str, any]]:
    """
    Main function to run valuation for multiple stock files and ETFs in parallel.

    Complex Business Logic:
    - Parallel Processing: Uses ThreadPoolExecutor for concurrent file processing
    - ETF Integration: Processes ETFs separately from regular stocks
    - Result Aggregation: Combines results from multiple sources into unified summary
    - Error Resilience: Continues processing even if individual valuations fail

    Processing Pipeline:
    1. Sequential ETF valuation (due to complex interdependencies)
    2. Parallel stock file processing
    3. Comprehensive result aggregation and reporting
    """
    all_valuation_results = []
    currency_symbol = "€"
    print_lock = threading.Lock()

    def process_single_file(stock_name: str, file_path: str) -> List[Dict]:
        """Process a single file"""
        file_results = []

        with print_lock:
            print(f"\n{'=' * 60}")
            print(f"PROCESSING: {stock_name} ({file_path})")
            print(f"{'=' * 60}")

        # Prepare data from CSV file
        try:
            weights, companies_to_evaluate = prepare_csv_data(file_path, companies_to_value)
            with print_lock:
                print(f"✓ Loaded weights for {len(weights)} companies from {stock_name}")
        except (FileNotFoundError, ValueError) as e:
            with print_lock:
                print(f"✗ Error loading file {stock_name}: {e}")
            return file_results

        comparable_companies = companies_to_evaluate

        # Value each company in the current file
        for base_ticker, peers in comparable_companies.items():
            if base_ticker not in weights:
                with print_lock:
                    print(f"⚠ Ticker {base_ticker} not found in file {stock_name}, skipping...")
                continue

            base_company_data = get_company_data(base_ticker, verbose=False)
            company_name = base_company_data.get('name', base_ticker)

            with print_lock:
                print(f"\n{'-' * 40}")
                print(f"VALUATION: {company_name} ({base_ticker}) - {stock_name}")
                print(f"{'-' * 40}")

            try:
                with print_lock:
                    print(f"[{stock_name}] Fetching peer company data for {base_ticker}...")
                multipliers = calculate_peer_multipliers(peers)

                if multipliers['peers_count'] == 0:
                    with print_lock:
                        print(f"✗ Failed to get data for any peers for {base_ticker}")
                    continue

                with print_lock:
                    print(f"[{stock_name}] Peers found: {multipliers['peers_count']}")
                    print(f"[{stock_name}] Calculating fair price for {base_ticker}...")

                valuation = valuate_company(base_ticker, multipliers, weights)

                if valuation['success']:
                    current = valuation['current_price']
                    fair = valuation['fair_price']
                    difference = ((fair - current) / current) * 100 if current else None

                    with print_lock:
                        print(
                            f"✔ [{stock_name}] {base_ticker} - Current: {currency_symbol}{current:,.2f}, Fair: {currency_symbol}{fair:,.2f}")

                    file_results.append({
                        "Type": "Stock",
                        "Source": stock_name,
                        "Ticker": base_ticker,
                        "Company": valuation['company_name'],
                        "Fair Price": fair,
                        "Current Price": current,
                        "Difference (%)": difference
                    })
                else:
                    with print_lock:
                        print(
                            f"✗ [{stock_name}] Valuation error for {base_ticker}: {valuation.get('error', 'Unknown error')}")

            except Exception as e:
                with print_lock:
                    print(f"✗ [{stock_name}] Critical error during valuation of {base_ticker}: {e}")

        with print_lock:
            print(f"✓ Completed processing file {stock_name}: {len(file_results)} companies valued")

        return file_results

    # Process ETFs SEPARATELY before processing stock files
    print(f"\n{'=' * 60}")
    print("PROCESSING ETFs")
    print(f"{'=' * 60}")

    etf_results = []
    for etf_name, etf_file in etf_dict.items():
        with print_lock:
            print(f"\nProcessing ETF: {etf_name}")

        res = calculate_etf_fair_value_wrapper(
            file_path=etf_file,
            comparable_map=companies_to_value
        )

        if res['success']:
            etf_results.append({
                "Type": "ETF",
                "Source": "ETF Portfolio",
                "Ticker": etf_name,
                "Company": f"ETF {etf_name}",
                "Fair Price": res['fair_value_etf'],
                "Current Price": res['current_price_etf'],
                "Difference (%)": res['premium_discount_pct']
            })
            with print_lock:
                print(f"✔ ETF {etf_name} processed successfully")
        else:
            with print_lock:
                print(f"✗ ETF valuation failed for {etf_name}: {res.get('error')}")

    all_valuation_results.extend(etf_results)

    # Parallel file processing for regular stocks
    print(f"\n🚀 Starting parallel processing of {len(stocks_dict)} stock files...")

    with ThreadPoolExecutor(max_workers=min(len(stocks_dict), 4)) as executor:
        future_to_file = {
            executor.submit(process_single_file, stock_name, file_path): stock_name
            for stock_name, file_path in stocks_dict.items()
        }

        for future in as_completed(future_to_file):
            stock_name = future_to_file[future]
            try:
                file_results = future.result()
                all_valuation_results.extend(file_results)
                print(f"✔ File {stock_name} processed successfully")
            except Exception as exc:
                print(f"✗ File {stock_name} raised an exception: {exc}")

    # --- Print Final Summary Table ---
    print(f"\n\n{'=' * 70}")
    print("FINAL VALUATION SUMMARY - ALL STOCKS & ETFs")
    print(f"{'=' * 70}")

    if not all_valuation_results:
        print("No companies were successfully valued.")
        return []

    sorted_results = sorted(all_valuation_results,
                            key=lambda x: x['Difference (%)'] if x['Difference (%)'] is not None else -math.inf,
                            reverse=True)

    df_results = pd.DataFrame(sorted_results)

    df_results['Fair Price'] = df_results['Fair Price'].apply(
        lambda x: f"{currency_symbol}{x:,.2f}" if pd.notna(x) else "N/A")
    df_results['Current Price'] = df_results['Current Price'].apply(
        lambda x: f"{currency_symbol}{x:,.2f}" if pd.notna(x) else "N/A")
    df_results['Difference (%)'] = df_results['Difference (%)'].apply(
        lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")

    print(df_results.to_string(index=False))

    print(f"\nTotal items valued: {len(all_valuation_results)}")
    print(f"  - ETFs: {len([r for r in all_valuation_results if r.get('Type') == 'ETF'])}")
    print(f"  - Stocks: {len([r for r in all_valuation_results if r.get('Type') == 'Stock'])}")
    print(f"Files processed: {len(stocks_dict)}")

    return all_valuation_results

# --- Main Execution ---
if __name__ == "__main__":
    results = run_valuation(stocks_dict,companies_to_value)