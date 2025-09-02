"""
5-Minute EMA Confirmation Engine
Analyzes standard candles for EMA crossover confirmations (Stage 2)
"""

"""
5-Minute EMA Confirmation Engine
Analyzes standard candles for EMA crossover confirmations (Stage 2)
"""

import json
import logging
import yaml
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from database.signal_manager import SignalManager
from alerts.two_stage_telegram import send_execution_alert
from utils.exchange_manager import fetch_ohlcv_data

logger = logging.getLogger(__name__)

def ema(series, period):
    """Calculate Exponential Moving Average"""
    return series.ewm(span=period, adjust=False).mean()

class EMAConfirmationAnalyzer5m:
    def __init__(self):
        self.config = self.load_config()
        self.signal_manager = SignalManager()
        # No direct exchange initialization - using exchange manager with fallback
        self.fast_ema_period = self.config['ema']['fast_period']  # 9
        self.slow_ema_period = self.config['ema']['slow_period']  # 18
        
    def load_config(self):
        """Load configuration from YAML file"""
        config_path = Path("config/enhanced_config.yaml")
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def load_cached_coins(self):
        """Load cached market data for getting market cap info"""
        cache_path = Path("cache/high_risk_market_data.json")
        try:
            with open(cache_path, 'r') as file:
                cache_data = json.load(file)
                return {coin['symbol']: coin for coin in cache_data.get('coins', [])}
        except Exception as e:
            logger.error(f"Error loading cached coins: {e}")
            return {}
    
    def get_5m_ohlc_data(self, symbol, limit=50):
        """
        Get 5-minute OHLC data using exchange manager with fallback
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            limit: Number of candles to fetch
            
        Returns:
            DataFrame: OHLC data or None if error
        """
        try:
            # Use exchange manager with fallback logic
            df = fetch_ohlcv_data(symbol, '5m', limit)
            
            if df is not None:
                # Convert column names for EMA calculation (lowercase)
                df = df.rename(columns={
                    'Open': 'open', 
                    'High': 'high', 
                    'Low': 'low', 
                    'Close': 'close',
                    'Volume': 'volume'
                })
                logger.debug(f"Fetched {len(df)} 5m candles for {symbol}")
            else:
                logger.warning(f"No 5m OHLCV data available for {symbol}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching 5m data for {symbol}: {e}")
            return None
    
    def calculate_emas(self, df):
        """
        Calculate 9 EMA and 18 EMA on close prices
        
        Args:
            df: DataFrame with OHLC data
            
        Returns:
            DataFrame: DataFrame with EMA columns added
        """
        try:
            if df.empty or len(df) < max(self.fast_ema_period, self.slow_ema_period):
                logger.warning("Insufficient data for EMA calculation")
                return df
            
            result_df = df.copy()
            
            # Calculate EMAs using your specified settings
            result_df['ema_fast'] = ema(df['close'], self.fast_ema_period)    # 9 EMA
            result_df['ema_slow'] = ema(df['close'], self.slow_ema_period)    # 18 EMA
            
            logger.debug(f"Calculated EMAs: {self.fast_ema_period} and {self.slow_ema_period}")
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating EMAs: {e}")
            return df
    
    def detect_ema_crossover(self, df, signal_direction):
        """
        Detect EMA crossover in the specified direction
        Based on your specification: 9 EMA crossing above/below 18 EMA
        
        Args:
            df: DataFrame with EMA data
            signal_direction: 'BUY' or 'SELL'
            
        Returns:
            bool: True if crossover detected in recent candles
        """
        try:
            if 'ema_fast' not in df.columns or 'ema_slow' not in df.columns:
                logger.error("EMA data not found in DataFrame")
                return False
            
            # Check last few candles for crossover (look at last 3 candles)
            for i in range(max(1, len(df) - 3), len(df)):
                if i < 1:
                    continue
                
                current_fast = df.iloc[i]['ema_fast']  # 9 EMA
                current_slow = df.iloc[i]['ema_slow']  # 18 EMA
                prev_fast = df.iloc[i-1]['ema_fast']  # 9 EMA
                prev_slow = df.iloc[i-1]['ema_slow']  # 18 EMA
                
                # Skip if any values are NaN
                if pd.isna(current_fast) or pd.isna(current_slow) or pd.isna(prev_fast) or pd.isna(prev_slow):
                    continue
                
                # Check for BUY crossover: 9 EMA crosses above 18 EMA
                if (signal_direction == 'BUY' and 
                    current_fast > current_slow and prev_fast <= prev_slow):
                    logger.info(f"BUY crossover detected: 9EMA ({current_fast:.6f}) crossed above 18EMA ({current_slow:.6f})")
                    return True
                
                # Check for SELL crossover: 9 EMA crosses below 18 EMA
                if (signal_direction == 'SELL' and 
                    current_fast < current_slow and prev_fast >= prev_slow):
                    logger.info(f"SELL crossover detected: 9EMA ({current_fast:.6f}) crossed below 18EMA ({current_slow:.6f})")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error detecting EMA crossover: {e}")
            return False
    
    def analyze_pending_signal(self, signal_data, coins_market_data):
        """
        Analyze a pending signal for EMA confirmation
        
        Args:
            signal_data: Signal data from database
            coins_market_data: Dictionary of coin market data
            
        Returns:
            dict: Confirmation data or None
        """
        try:
            symbol = signal_data['coin_symbol']
            signal_direction = signal_data['signal_direction']
            
            # Get 5m OHLC data using exchange manager
            ohlc_df = self.get_5m_ohlc_data(symbol)
            if ohlc_df is None or ohlc_df.empty:
                logger.warning(f"No 5m data available for {symbol}")
                return None
            
            # Calculate EMAs
            df_with_emas = self.calculate_emas(ohlc_df)
            
            # Check for crossover
            has_crossover = self.detect_ema_crossover(df_with_emas, signal_direction)
            
            if not has_crossover:
                return None
            
            # Get current price and market data
            current_price = df_with_emas.iloc[-1]['close']
            market_data = coins_market_data.get(symbol, {})
            
            confirmation_data = {
                'symbol': symbol,
                'signal_direction': signal_direction,
                'current_price': current_price,
                'market_cap': market_data.get('market_cap', 0),
                'price_change_percentage_24h': market_data.get('price_change_percentage_24h', 0),
                'ema_fast': df_with_emas.iloc[-1]['ema_fast'],
                'ema_slow': df_with_emas.iloc[-1]['ema_slow'],
                'confirmation_timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"EMA confirmation for {symbol} {signal_direction}")
            return confirmation_data
            
        except Exception as e:
            logger.error(f"Error analyzing pending signal for {signal_data.get('coin_symbol', 'UNKNOWN')}: {e}")
            return None
    
    def process_pending_signals(self):
        """
        Process all pending signals for EMA confirmations
        
        Returns:
            tuple: (buy_confirmations, sell_confirmations)
        """
        buy_confirmations = []
        sell_confirmations = []
        
        try:
            # Get pending signals
            pending_signals = self.signal_manager.get_pending_signals()
            if not pending_signals:
                logger.info("No pending signals to process")
                return buy_confirmations, sell_confirmations
            
            logger.info(f"Processing {len(pending_signals)} pending signals...")
            
            # Load market data for coin info
            coins_market_data = self.load_cached_coins()
            
            for signal_data in pending_signals:
                try:
                    confirmation_data = self.analyze_pending_signal(signal_data, coins_market_data)
                    
                    if confirmation_data:
                        # Confirm and delete the signal
                        success = self.signal_manager.confirm_signal(signal_data['coin_symbol'])
                        
                        if success:
                            # Categorize confirmation
                            if confirmation_data['signal_direction'] == 'BUY':
                                buy_confirmations.append(confirmation_data)
                            elif confirmation_data['signal_direction'] == 'SELL':
                                sell_confirmations.append(confirmation_data)
                        
                except Exception as e:
                    logger.error(f"Error processing signal: {e}")
                    continue
            
            logger.info(f"Found {len(buy_confirmations)} BUY and {len(sell_confirmations)} SELL confirmations")
            
        except Exception as e:
            logger.error(f"Error processing pending signals: {e}")
        
        return buy_confirmations, sell_confirmations
    
    def send_alerts(self, buy_confirmations, sell_confirmations):
        """
        Send Stage 2 execution alerts
        
        Args:
            buy_confirmations: List of BUY confirmations
            sell_confirmations: List of SELL confirmations
            
        Returns:
            bool: True if alerts sent successfully
        """
        try:
            if buy_confirmations or sell_confirmations:
                success = send_execution_alert(buy_confirmations, sell_confirmations)
                if success:
                    logger.info("Stage 2 execution alerts sent successfully")
                else:
                    logger.error("Failed to send Stage 2 execution alerts")
                return success
            else:
                logger.info("No confirmations to alert")
                return True
                
        except Exception as e:
            logger.error(f"Error sending alerts: {e}")
            return False

def analyze_5m():
    """Main function for 5-minute analysis"""
    try:
        analyzer = EMAConfirmationAnalyzer5m()
        
        # Process pending signals for confirmations
        buy_confirmations, sell_confirmations = analyzer.process_pending_signals()
        
        # Send alerts if any confirmations found
        if buy_confirmations or sell_confirmations:
            success = analyzer.send_alerts(buy_confirmations, sell_confirmations)
            if not success:
                logger.error("Failed to send alerts")
                return False
        
        logger.info("5-minute analysis completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in 5-minute analysis: {e}")
        return False

if __name__ == "__main__":
    analyze_5m()
