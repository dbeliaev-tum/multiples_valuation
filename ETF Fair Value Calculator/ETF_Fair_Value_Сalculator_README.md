# ETF Fair Value Calculator

## Overview

A comprehensive Python tool for calculating the fair value of Exchange Traded Funds (ETFs) using bottom-up, peer-based valuation methodology. The script values each ETF constituent individually and aggregates results to determine overall ETF fair value.

## Features

- **Multi-Method Valuation**: Uses EV/EBITDA, P/E, and P/S ratios with configurable weights
- **Peer Comparison Analysis**: Automatically fetches and analyzes comparable companies
- **Multi-Currency Support**: Handles international stocks with automatic EUR conversion
- **Parallel Processing**: Uses ThreadPoolExecutor for efficient data fetching
- **Intelligent Caching**: Implements LRU caching to minimize API calls
- **Robust Error Handling**: Continues processing even with partial data failures
- **Weight Redistribution**: Automatically adjusts weights when valuation methods are unavailable

## Installation

1. Ensure Python 3.7+ is installed
2. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Required Dependencies

`requirements.txt` file:
```txt
yfinance>=0.2.18
pandas>=1.5.0
numpy>=1.21.0
```

## Configuration

### 1. ETF File Configuration

Modify the `etf_dict` in the script to point to your CSV files:

```python
etf_dict = {
    "DAX": "Investments - DAX.csv",
    "POLAND": "msci_poland.csv",
    # Add more ETFs as needed
}
```

### 2. Comparable Companies Setup

Define peer groups in the `companies_to_value` dictionary:

```python
companies_to_value = {
    "KO": ["PEP", "KDP", "MNST"],  # Coca-Cola and its peers
    "AAPL": ["MSFT", "GOOGL"],     # Apple and its peers
    # Add more companies and their comparables
}
```

### 3. CSV File Format

Your ETF CSV files must contain these columns:

| Column | Description                          | Format |
|--------|--------------------------------------|---------|
| `ticker` | Stock symbol                         | String |
| `share` | Share of the company in ETF          | Decimal |
| `price_ev_w` | Weight for EV/EBITDA valuation (0-1) | Decimal |
| `price_pe_w` | Weight for P/E valuation (0-1)       | Decimal |
| `price_ps_w` | Weight for P/S valuation (0-1)       | Decimal |

**Example CSV structure:**
```csv
ticker,share,price_ev_w,price_pe_w,price_ps_w
AAPL,0.5,0.183,0.35,0.467
MSFT,0.5,0.183,0.35,0.467
```

## Usage

Run the script directly:

```bash
python etf1.py
```

### What Happens When You Run the Script:

1. **ETF Processing**: Each ETF in `etf_dict` is processed sequentially
2. **Data Collection**: Financial data is fetched for all constituent companies
3. **Peer Analysis**: Valuation multiples are calculated from comparable companies
4. **Individual Valuation**: Each company is valued using multiple methods
5. **ETF Aggregation**: Results are aggregated to calculate ETF-level valuation
6. **Results Display**: Comprehensive results are printed to console

## Output Example

```
--- 🔍 Starting ETF analysis with 15 tickers ---
✔ Successfully valued AAPL: price 150.25 → fair 158.75
✔ Successfully valued MSFT: price 305.50 → fair 295.80
...

--- 💰 FINAL ETF VALUATION RESULT ---
✔ ETF Current Value: €100.00
✔ ETF Fair Value: €104.40
✔ Premium/Discount: +4.4%
✔ Successfully Valued: 39/39 companies
```

## Valuation Methodology

### 1. Data Collection
- Fetches real-time data from Yahoo Finance API
- Handles multiple currencies with live exchange rates
- Implements retry logic for API failures

### 2. Peer Multiplier Calculation
For each company, calculates average multiples from peers:
- **EV/EBITDA**: Enterprise Value to Earnings Before Interest, Taxes, Depreciation, and Amortization
- **P/E**: Price to Earnings ratio
- **P/S**: Price to Sales ratio

### 3. Individual Company Valuation
Applies three valuation methods with intelligent weight management:

```python
# Weight redistribution example:
Initial weights: [0.33, 0.33, 0.33]  # [EV/EBITDA, P/E, P/S]
If P/E data unavailable → New weights: [0.55, 0, 0.45]
```

### 4. ETF-Level Aggregation
- Sums current market values of all constituents
- Sums calculated fair values of all constituents
- Calculates overall premium/discount percentage

## Error Handling

The script is designed to be resilient:

- **Partial Failures**: Continues processing even if some companies fail
- **Data Validation**: Checks for valid prices, share counts, and financial metrics
- **Graceful Degradation**: Provides results with available data
- **Detailed Reporting**: Shows exactly which companies failed and why

## Customization Options

### Valuation Parameters
- Modify multiplier bounds in `calculate_peer_multipliers()`
- Adjust weight redistribution logic in `valuate_company()`
- Change currency conversion pairs

### Performance Tuning
- Adjust `max_workers` in ThreadPoolExecutor
- Modify cache size in `@lru_cache` decorators
- Change API delay timing between requests

### Data Sources
- Extend financial metric keys for different data formats
- Add alternative data validation rules
- Implement additional valuation methods

## Troubleshooting

### Common Issues:

1. **Missing Data**: Some companies may lack financial metrics
   - Solution: The script automatically redistributes weights to available methods

2. **API Rate Limiting**: Yahoo Finance may throttle requests
   - Solution: Built-in delays and caching minimize this issue

3. **Currency Conversion Failures**: Exchange rates unavailable
   - Solution: Falls back to 1:1 conversion with warning

4. **File Format Issues**: CSV files not formatted correctly
   - Solution: Check column names and number formatting

### Debug Mode:
Use the built-in diagnostic function:
```python
test_peer_data_directly(["PEP", "KDP", "MNST"], "KO")
```

## Limitations

- Dependent on Yahoo Finance data availability and accuracy
- Valuation multiples may be skewed by outlier companies
- No adjustment for company-specific risk factors or growth prospects
- Historical data limitations for some financial metrics

## Support

For issues or questions:
1. Check the console output for detailed error messages
2. Verify CSV file formatting and column names
3. Ensure all ticker symbols are valid and tradable
4. Check internet connectivity for API calls

## License

MIT License
