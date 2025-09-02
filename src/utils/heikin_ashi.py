"""
Heikin-Ashi Candlestick Utilities
Converts regular OHLC data to Heikin-Ashi format
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def convert_to_heikin_ashi(df):
    """
    Convert regular OHLC DataFrame to Heikin-Ashi
    
    Args:
        df: DataFrame with columns ['Open', 'High', 'Low', 'Close', 'Volume']
        
    Returns:
        DataFrame with Heikin-Ashi OHLC values
    """
    try:
        if df.empty:
            logger.warning("Empty DataFrame provided for Heikin-Ashi conversion")
            return df
        
        # Create copy to avoid modifying original
        ha_df = df.copy()
        
        # Ensure proper column names (uppercase)
        column_mapping = {
            'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
            'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in ha_df.columns:
                ha_df[new_col] = ha_df[old_col]
        
        # Initialize first row
        ha_df.loc[ha_df.index[0], 'ha_close'] = (ha_df.iloc[0]['Open'] + ha_df.iloc[0]['High'] + 
                                                 ha_df.iloc[0]['Low'] + ha_df.iloc[0]['Close']) / 4
        ha_df.loc[ha_df.index[0], 'ha_open'] = (ha_df.iloc[0]['Open'] + ha_df.iloc[0]['Close']) / 2
        ha_df.loc[ha_df.index[0], 'ha_high'] = ha_df.iloc[0]['High']
        ha_df.loc[ha_df.index[0], 'ha_low'] = ha_df.iloc[0]['Low']
        
        # Calculate subsequent rows
        for i in range(1, len(ha_df)):
            idx = ha_df.index[i]
            prev_idx = ha_df.index[i-1]
            
            # Heikin-Ashi Close = (O + H + L + C) / 4
            ha_df.loc[idx, 'ha_close'] = (ha_df.iloc[i]['Open'] + ha_df.iloc[i]['High'] + 
                                         ha_df.iloc[i]['Low'] + ha_df.iloc[i]['Close']) / 4
            
            # Heikin-Ashi Open = (Previous HA Open + Previous HA Close) / 2
            ha_df.loc[idx, 'ha_open'] = (ha_df.loc[prev_idx, 'ha_open'] + 
                                        ha_df.loc[prev_idx, 'ha_close']) / 2
            
            # Heikin-Ashi High = Max(High, HA Open, HA Close)
            ha_df.loc[idx, 'ha_high'] = max(ha_df.iloc[i]['High'], 
                                           ha_df.loc[idx, 'ha_open'], 
                                           ha_df.loc[idx, 'ha_close'])
            
            # Heikin-Ashi Low = Min(Low, HA Open, HA Close)
            ha_df.loc[idx, 'ha_low'] = min(ha_df.iloc[i]['Low'], 
                                          ha_df.loc[idx, 'ha_open'], 
                                          ha_df.loc[idx, 'ha_close'])
        
        # Replace original OHLC with Heikin-Ashi values
        ha_df['Open'] = ha_df['ha_open']
        ha_df['High'] = ha_df['ha_high']
        ha_df['Low'] = ha_df['ha_low']
        ha_df['Close'] = ha_df['ha_close']
        
        # Drop temporary columns
        ha_df = ha_df.drop(['ha_open', 'ha_high', 'ha_low', 'ha_close'], axis=1)
        
        logger.debug(f"Successfully converted {len(df)} candles to Heikin-Ashi")
        return ha_df
        
    except Exception as e:
        logger.error(f"Error converting to Heikin-Ashi: {e}")
        raise

def is_valid_ohlc(df):
    """
    Validate that DataFrame contains required OHLC columns
    
    Args:
        df: DataFrame to validate
        
    Returns:
        bool: True if valid OHLC data
    """
    required_columns = ['Open', 'High', 'Low', 'Close']
    
    if df.empty:
        return False
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for non-numeric data
    for col in required_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            logger.error(f"Column {col} contains non-numeric data")
            return False
    
    # Check for negative prices
    if (df[required_columns] < 0).any().any():
        logger.error("Negative price values found")
        return False
    
    return True

def prepare_heikin_ashi_data(ohlc_data, symbol):
    """
    Prepare and validate OHLC data for Heikin-Ashi conversion
    
    Args:
        ohlc_data: Raw OHLC data (dict, list, or DataFrame)
        symbol: Trading symbol for logging
        
    Returns:
        DataFrame: Processed Heikin-Ashi data or None if error
    """
    try:
        # Convert to DataFrame if needed
        if isinstance(ohlc_data, (list, dict)):
            df = pd.DataFrame(ohlc_data)
        elif isinstance(ohlc_data, pd.DataFrame):
            df = ohlc_data.copy()
        else:
            logger.error(f"Unsupported data type for {symbol}: {type(ohlc_data)}")
            return None
        
        if df.empty:
            logger.warning(f"No OHLC data available for {symbol}")
            return None
        
        # Ensure required columns exist (handle both cases)
        if 'open' in df.columns:
            df['Open'] = df['open']
            df['High'] = df['high']
            df['Low'] = df['low']
            df['Close'] = df['close']
        
        # Validate OHLC data
        if not is_valid_ohlc(df):
            logger.error(f"Invalid OHLC data for {symbol}")
            return None
        
        # Convert to Heikin-Ashi
        ha_df = convert_to_heikin_ashi(df)
        
        logger.info(f"Prepared Heikin-Ashi data for {symbol}: {len(ha_df)} candles")
        return ha_df
        
    except Exception as e:
        logger.error(f"Error preparing Heikin-Ashi data for {symbol}: {e}")
        return None

if __name__ == "__main__":
    # Test with sample data
    sample_data = pd.DataFrame({
        'Open': [100, 102, 101, 103],
        'High': [105, 104, 105, 106],
        'Low': [99, 100, 99, 102],
        'Close': [102, 101, 103, 105],
        'Volume': [1000, 1200, 1100, 1300]
    })
    
    print("Original OHLC:")
    print(sample_data)
    
    ha_data = convert_to_heikin_ashi(sample_data)
    print("\nHeikin-Ashi OHLC:")
    print(ha_data)
