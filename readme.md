# Market Multiples Valuation Script

## Overview

A sophisticated Python-based financial analysis tool that performs relative valuation of companies using peer-based market multiples methodology. This script calculates fair values for individual stocks and entire ETFs by comparing them to comparable companies using industry-standard valuation metrics.

## Features

### 📊 Multi-Method Valuation
- **EV/EBITDA**: Enterprise Value to Earnings Before Interest, Taxes, Depreciation, and Amortization
- **P/E Ratio**: Price to Earnings ratio analysis
- **P/S Ratio**: Price to Sales ratio evaluation
- **Configurable Weights**: Customizable weighting for each valuation method

### 🌍 Global Market Support
- **Multi-Currency Handling**: Automatic conversion to EUR using live exchange rates
- **International Tickers**: Support for global stock exchanges (US, EU, Japan, etc.)
- **Localized Number Formatting**: Handles European decimal and thousands separators

### ⚡ Performance Optimized
- **Parallel Processing**: Concurrent data fetching using ThreadPoolExecutor
- **Intelligent Caching**: LRU caching to minimize API calls and improve performance
- **Rate Limit Management**: Strategic delays to respect Yahoo Finance API limits

### 🔍 Comprehensive Analysis
- **Peer Comparison**: Automated comparable company analysis
- **ETF Valuation**: Bottom-up ETF valuation aggregating constituent stocks
- **Weight Redistribution**: Automatic adjustment when valuation methods are unavailable
- **Detailed Reporting**: Comprehensive valuation summaries with premium/discount analysis

## Installation

### Prerequisites
- Python 3.7 or higher
- pip package manager

### Dependencies Installation
```bash
pip install yfinance pandas numpy
```

## Configuration

### 1. Company Peer Groups
Define comparable companies in the `companies_to_value` dictionary:

```python
companies_to_value = {
    "AAPL": ["MSFT", "GOOGL", "ORCL"],
    "MSFT": ["AAPL", "ORCL", "SAP.DE"],
    "BRK-B": ["BLK", "MKL", "JEF"],
    # Add more companies and their peers
}
```

### 2. ETF Configuration
Map ETF names to their CSV data files:

```python
etf_dict = {
    "DAX": "Investments - DAX.csv",
    "POLAND": "msci_poland.csv",
}
```

### 3. Stock Portfolio Files
Define stock portfolio files for analysis:

```python
stocks_dict = {
    "Test": "Investments - superdeals.csv",
    "Portfolio_A": "portfolio_a.csv"
}
```

## Stock Portfolio CSV File Format

### Required Columns
Your CSV files must contain these exact column names:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `ticker` | String | Stock symbol | "AAPL"  |
| `price_ev_w` | Decimal (0-1) | Weight for EV/EBITDA valuation | 0.183   |
| `price_pe_w` | Decimal (0-1) | Weight for P/E valuation | 0.35    |
| `price_ps_w` | Decimal (0-1) | Weight for P/S valuation | 0.467   |

### Example CSV Structure
```csv
ticker,price_ev_w,price_pe_w,price_ps_w
AAPL,0.183,0.35,0.467
MSFT,0.183,0.35,0.467
BRK-B,0.017,0.583,0.4
```

## Usage

### Basic Execution
```bash
python Invest1.py
```

### What the Script Does

1. **Data Collection Phase**
   - Fetches real-time financial data from Yahoo Finance API
   - Retrieves key metrics: Price, Shares, Debt, Cash, EBITDA, Revenue, EPS
   - Converts all currencies to EUR for consistent comparison

2. **Peer Analysis Phase**
   - Calculates valuation multiples for each peer company
   - Applies statistical filtering to remove outliers
   - Computes average multiples for each valuation method

3. **Valuation Phase**
   - Applies peer multiples to target companies
   - Uses weighted average of available valuation methods
   - Automatically redistributes weights when methods are unavailable

4. **Reporting Phase**
   - Generates comprehensive valuation results
   - Calculates premium/discount percentages
   - Provides detailed method-level breakdowns

## Output Example

```
============================================================
FINAL VALUATION SUMMARY - ALL STOCKS & ETFs
============================================================

Type  Source    Ticker  Company          Fair Price    Current Price  Difference
----  --------  ------  ---------------  ------------  -------------  -----------
Stock Test      AAPL    Apple Inc.       €158.75       €150.25        +5.7%
ETF   ETF       DAX     ETF DAX          €104.40       €100.00        +4.4%
Stock Test      MSFT    Microsoft Corp.  €295.80       €305.50        -3.2%

Total items valued: 3
  - ETFs: 1
  - Stocks: 2
Files processed: 3
```

## Valuation Methodology

### Enterprise Value Calculation
```
EV = Market Capitalization + Total Debt - Cash & Equivalents
   = (Price × Shares Outstanding) + Debt - Cash
```

### Multiplier Formulas
- **EV/EBITDA**: `EV / EBITDA`
- **P/E Ratio**: `Price / Earnings Per Share`
- **P/S Ratio**: `Price / (Revenue / Shares Outstanding)`

### Fair Price Calculation
- **EV/EBITDA Method**: `Fair Price = (Peer EV/EBITDA × Company EBITDA - Debt + Cash) / Shares`
- **P/E Method**: `Fair Price = Peer P/E × Company EPS`
- **P/S Method**: `Fair Price = Peer P/S × (Company Revenue / Shares)`

### Weight Redistribution Logic
The script intelligently handles missing data by redistributing weights:

```
Initial weights: [EV/EBITDA: 0.33, P/E: 0.33, P/S: 0.33]
If P/E data unavailable → New weights: [0.55, 0, 0.45]
```

## Error Handling

The script is designed to be robust and continue processing even with partial data:

- **API Failures**: Continues with available data when Yahoo Finance fails
- **Missing Metrics**: Automatically excludes unavailable valuation methods
- **Data Validation**: Comprehensive checks for valid prices, shares, and financial metrics
- **Graceful Degradation**: Provides partial results rather than complete failure

## Customization

### Valuation Parameters
Modify these constants in the script:
```python
# Multiplier bounds for outlier filtering
EV_EBITDA_MAX = 50    # Exclude EV/EBITDA > 50
P_E_MAX = 100         # Exclude P/E > 100  
P_S_MAX = 40          # Exclude P/S > 40

# Default weights when not specified
DEFAULT_WEIGHTS = (0.33, 0.33, 0.33)
```

### Performance Tuning
```python
# Threading configuration
MAX_WORKERS = 10      # Parallel threads for data fetching
API_DELAY = 0.1       # Delay between API calls (seconds)

# Caching settings
CACHE_SIZE = None     # LRU cache size (None = unlimited)
```

## Troubleshooting

### Common Issues

1. **Missing Data for Some Companies**
   - *Cause:* Yahoo Finance may not have complete data for all tickers
   - *Solution:* The script automatically excludes companies with insufficient data

2. **API Rate Limiting**
   - *Cause:* Too many rapid requests to Yahoo Finance
   - *Solution:* Built-in delays and caching minimize this issue

3. **Currency Conversion Failures**
   - *Cause:* Exchange rate data temporarily unavailable
   - *Solution:* Falls back to 1:1 conversion with warning

4. **CSV Format Issues**
   - *Solution:* Ensure exact column names and proper number formatting

### Debug Mode
Enable verbose output by modifying function calls:
```python
# Change verbose=False to verbose=True for detailed debugging
data = get_company_data(ticker, verbose=True)
```

## Financial Metrics Explained

### Key Data Points Collected
- **Price**: Current stock price (converted to EUR)
- **Shares**: Shares outstanding for market cap calculation
- **Debt**: Total debt for enterprise value calculation
- **Cash**: Cash and equivalents for enterprise value
- **EBITDA**: Earnings Before Interest, Taxes, Depreciation, Amortization
- **Revenue**: Total revenue for sales-based valuation
- **EPS**: Earnings Per Share for earnings-based valuation

## Limitations

- **Data Availability**: Dependent on Yahoo Finance data completeness and accuracy
- **Historical Data**: Uses current financial metrics only (no historical averaging)
- **Industry Adjustments**: No automatic industry-specific adjustments to multiples
- **Growth Considerations**: Does not account for differential growth rates between companies

## Support

For issues or questions:
1. Check the console output for detailed error messages
2. Verify all ticker symbols are valid and currently trading
3. Ensure CSV files follow the exact required format
4. Confirm internet connectivity for API calls

## License

MIT License

## Contributing

Contributions are welcome! Please ensure:
- All new code includes type hints and comprehensive docstrings
- Financial calculations are thoroughly tested
- Documentation is updated accordingly