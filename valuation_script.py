# Market Multiples Valuation Script
#
# This script performs a relative valuation of companies by comparing them to a set of peers
# using key market multiples like EV/EBITDA, P/E, and P/S. It fetches financial data from Yahoo Finance,
# calculates average peer multiples, and then applies these to the target company to
# estimate its fair value.
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
    # Read the CSV file with proper handling of quotes and European number format
    df = pd.read_csv('sample_deals_yahoo.csv',
                     sep=',',
                     decimal=',',
                     thousands='.',
                     quotechar='"',
                     encoding='utf-8')

    # Remove completely empty rows
    df = df.dropna(how='all')

    # # Display the first few rows to check the structure
    # print("DataFrame structure:")
    # print(df.head())
    # print("\nColumns in DataFrame:")
    # print(df.columns.tolist())

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

# Check if we have any data after filtering
if combined_df.empty:
    print("✗ Error: No data found after filtering for required columns.")
    print("Available data in required columns:")
    for col in required_columns:
        print(f"{col}: {df[col].notna().sum()} non-null values")
    exit()

# Convert numerical columns to float, handling potential comma-based decimals
try:
    for col in ['price_ev_w', 'price_pe_w', 'price_ps_w']:
        # First convert to string, then replace comma with dot, then to float
        combined_df.loc[:, col] = combined_df[col].astype(str).str.replace(',', '.').astype(float)
except ValueError as e:
    print(f"✗ Error converting data to numeric format: {e}")
    print("Sample values in problematic column:")
    print(combined_df[col].head())
    exit()

# Convert the DataFrame to a dictionary mapping tickers to their weights
weights = {}
for _, row in combined_df.iterrows():
    ticker = row['ticker']
    weights[ticker] = (row['price_ev_w'], row['price_pe_w'], row['price_ps_w'])

# A new dictionary to store only the companies that have corresponding data in the CSV file
companies_to_evaluate = {}
for ticker in combined_df['ticker'].unique():
    if ticker in companies_to_value:
        companies_to_evaluate[ticker] = companies_to_value[ticker]
    else:
        print(f"Ticker {ticker} is not in the peer valuation database.")
        continue

# Debug information
# print(f"Companies to evaluate: {list(companies_to_evaluate.keys())}")
# print(f"Weights: {weights}")

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