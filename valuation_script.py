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
        price_f = safe_float(price) * euro_rate
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

def calculate_peer_multipliers(peers: List[str]) -> Dict:
    """Calculates the average market multiples (EV/EBITDA, P/E, P/S) for a list of peer companies."""
    ev_ebitda_list = []
    p_e_list = []
    p_s_list = []
    successful_peers = []
    failed_to_get_data = []

    for ticker in peers:
        # Use verbose=False to avoid cluttered output for peers
        data = get_company_data(ticker, verbose=False)
        if not data['success']:
            failed_to_get_data.append(ticker)
            continue

        is_calculable = False

        # EV/EBITDA calculation
        if all(data.get(x) is not None for x in ['price', 'shares', 'debt', 'cash', 'ebitda']) and data['ebitda'] != 0:
            ev = data['price'] * data['shares'] + data['debt'] - data['cash']
            multiple = ev / data['ebitda']
            if 0 < multiple < 50:
                ev_ebitda_list.append(multiple)
                is_calculable = True

        # P/E calculation
        if data.get('price') is not None and data.get('eps') is not None and data['eps'] != 0:
            multiple = data['price'] / data['eps']
            if 0 < multiple < 100:
                p_e_list.append(multiple)
                is_calculable = True

        # P/S calculation
        if (data.get('price') is not None and data.get('revenue') is not None and
                data.get('shares') is not None and data['shares'] != 0):
            sales_per_share = data['revenue'] / data['shares']
            if sales_per_share != 0:
                multiple = data['price'] / sales_per_share
                if 0 < multiple < 40:
                    p_s_list.append(multiple)
                is_calculable = True

        if is_calculable:
            successful_peers.append(data['name'])
            print(f"✔ Processed: {data['name']} ({ticker})")
        else:
            failed_to_get_data.append(data['name'])

    # Calculate average multiples
    result = {
        'ev_ebitda': sum(ev_ebitda_list) / len(ev_ebitda_list) if ev_ebitda_list else None,
        'p_e': sum(p_e_list) / len(p_e_list) if p_e_list else None,
        'p_s': sum(p_s_list) / len(p_s_list) if p_s_list else None,
        'successful_peers': successful_peers,
        'peers_count': len(successful_peers)
    }

    print("\n--- AVERAGE MULTIPLES ---")
    print(f"Average P/E: {result['p_e']:.2f}" if result['p_e'] else "N/A (No peer data)")
    print(f"Average P/S: {result['p_s']:.2f}" if result['p_s'] else "N/A (No peer data)")
    print(f"Average EV/EBITDA: {result['ev_ebitda']:.2f}" if result['ev_ebitda'] else "N/A (No peer data)")
    print("-------------------------")

    if failed_to_get_data:
        print(f"⚠️ The following companies could not be used (no data): {', '.join(failed_to_get_data)}")

    return result

def valuate_company(ticker: str, multipliers: Dict, weights: Dict) -> Dict:
    """
    Calculates a company's fair value based on average peer multiples.

    Args:
        ticker: The ticker of the target company.
        multipliers: A dictionary of average peer multiples.
        weights: A dictionary with user-defined weights for each ticker.
    """
    data = get_company_data(ticker, verbose=True)
    if not data['success']:
        return {'success': False, 'error': 'Failed to fetch target company data'}

    # Get the valuation weights for the current ticker. Use default weights if not specified.
    w_ev_ebitda, w_pe, w_ps = weights.get(ticker, (0.33, 0.33, 0.33))

    final_fair_price = 0
    total_weights = 0
    calculations = {}

    # Check for missing data and re-distribute weights if necessary
    if multipliers['ev_ebitda'] is None or data['ebitda'] is None or data['shares'] is None or data['shares'] == 0:
        print("⚠️ Insufficient data for EV/EBITDA. Its weight will be reallocated.")
        if w_pe > 0:
            w_pe += w_ev_ebitda / 2
        if w_ps > 0:
            w_ps += w_ev_ebitda / 2
        w_ev_ebitda = 0

    # EV/EBITDA method
    if w_ev_ebitda > 0:
        ev = multipliers['ev_ebitda'] * data['ebitda']
        # Handle potential zero values
        if data['shares'] != 0:
            price_ev = (ev - (data['debt'] or 0) + (data['cash'] or 0)) / data['shares']
            if price_ev > 0:
                final_fair_price += price_ev * w_ev_ebitda
                total_weights += w_ev_ebitda
                calculations['ev_ebitda'] = price_ev

    # P/E method
    if multipliers['p_e'] is None or data['eps'] is None:
        print("⚠️ Insufficient data for P/E. This method will not be used.")
        w_pe = 0

    if w_pe > 0:
        price_pe = multipliers['p_e'] * data['eps']
        if price_pe > 0:
            final_fair_price += price_pe * w_pe
            total_weights += w_pe
            calculations['p_e'] = price_pe

    # P/S method
    if multipliers['p_s'] is None or data['revenue'] is None or data['shares'] is None or data['shares'] == 0:
        print("⚠️ Insufficient data for P/S. This method will not be used.")
        w_ps = 0

    if w_ps > 0:
        if data['shares'] != 0:
            sales_per_share = data['revenue'] / data['shares']
            if sales_per_share != 0:
                price_ps = multipliers['p_s'] * sales_per_share
                if price_ps > 0:
                    final_fair_price += price_ps * w_ps
                    total_weights += w_ps
                    calculations['p_s'] = price_ps

    if total_weights == 0:
        return {'success': False, 'error': 'Not enough data to perform valuation'}

    # Normalize weights and calculate the final weighted price
    final_price = final_fair_price / total_weights

    return {
        'success': True,
        'ticker': ticker,
        'company_name': data['name'],
        'current_price': data['price'],
        'fair_price': round(final_price, 2),
        'premium_discount': round((final_price / data['price'] - 1) * 100, 1) if data['price'] else None,
        'calculations': calculations,
        'peers_used': multipliers['successful_peers'],
        'peers_count': multipliers['peers_count']
    }

def run_valuation(comparable_companies: Dict[str, List[str]], weights: Dict) -> List[Dict]:
    """
    Main function to run the valuation for a dictionary of companies.

    Args:
        comparable_companies: A dictionary mapping target tickers to a list of peer tickers.
        weights: A dictionary of valuation weights for each ticker.
    """
    valuation_results = []
    currency_symbol = "€"

    for base_ticker, peers in comparable_companies.items():
        base_company_data = get_company_data(base_ticker, verbose=False)
        company_name = base_company_data.get('name', base_ticker)

        print(f"\n{'=' * 50}")
        print(f"VALUATION: {company_name} ({base_ticker})")
        print(f"{'=' * 50}")

        try:
            print("Fetching peer company data...")
            multipliers = calculate_peer_multipliers(peers)

            if multipliers['peers_count'] == 0:
                print(f"✗ Failed to get data for any peers for {base_ticker}")
                continue

            print(f"\nPeers found: {multipliers['peers_count']}")

            print("Calculating fair price...")
            valuation = valuate_company(base_ticker, multipliers, weights)

            if valuation['success']:
                current = valuation['current_price']
                fair = valuation['fair_price']
                difference = ((fair - current) / current) * 100 if current else None

                print(f"✔ Current Price: {currency_symbol}{current:,.2f}")
                print(f"✔ Fair Price: {currency_symbol}{fair:,.2f}")

                valuation_results.append({
                    "Ticker": base_ticker,
                    "Company": valuation['company_name'],
                    "Fair Price": fair,
                    "Current Price": current,
                    "Difference (%)": difference
                })
            else:
                print(f"✗ Valuation error: {valuation.get('error', 'Unknown error')}")

        except Exception as e:
            error_msg = f"Critical error during valuation of {base_ticker}: {e}"
            print(f"✗ {error_msg}")

    # --- Print Final Summary Table ---
    print(f"\n\n{'=' * 50}")
    print("FINAL VALUATION SUMMARY")
    print(f"{'=' * 50}")

    if not valuation_results:
        print("No companies were successfully valued.")
        return []

    # Sort results by the percentage difference in descending order
    sorted_results = sorted(valuation_results,
                            key=lambda x: x['Difference (%)'] if x['Difference (%)'] is not None else -math.inf,
                            reverse=True)

    # Create a DataFrame for a clean output
    df_results = pd.DataFrame(sorted_results)

    # Format columns for better readability
    df_results['Fair Price'] = df_results['Fair Price'].apply(
        lambda x: f"{currency_symbol}{x:,.2f}" if pd.notna(x) else "N/A")
    df_results['Current Price'] = df_results['Current Price'].apply(
        lambda x: f"{currency_symbol}{x:,.2f}" if pd.notna(x) else "N/A")
    df_results['Difference (%)'] = df_results['Difference (%)'].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")

    # Print the final table
    print(df_results.to_string(index=False))

    return valuation_results

# --- Main Execution ---
if __name__ == "__main__":
    results = run_valuation(companies_to_evaluate, weights)