import pandas as pd

def heikin_ashi(df):
    """
    Convert regular OHLC data to Heikin-Ashi candles
    """
    if df.empty:
        return df
    
    ha_df = pd.DataFrame(index=df.index)
    
    # HA Close = (O + H + L + C) / 4
    ha_df['Close'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
    
    # Initialize HA Open
    ha_df['Open'] = 0.0
    
    # First HA Open = (First O + First C) / 2
    ha_df.iloc[0, ha_df.columns.get_loc('Open')] = (df.iloc[0]['Open'] + df.iloc[0]['Close']) / 2
    
    # Subsequent HA Open = (Previous HA Open + Previous HA Close) / 2
    for i in range(1, len(ha_df)):
        ha_df.iloc[i, ha_df.columns.get_loc('Open')] = (
            ha_df.iloc[i-1]['Open'] + ha_df.iloc[i-1]['Close']
        ) / 2
    
    # HA High = max(HA Open, HA Close, High)
    ha_df['High'] = pd.concat([ha_df['Open'], ha_df['Close'], df['High']], axis=1).max(axis=1)
    
    # HA Low = min(HA Open, HA Close, Low)
    ha_df['Low'] = pd.concat([ha_df['Open'], ha_df['Close'], df['Low']], axis=1).min(axis=1)
    
    return ha_df
