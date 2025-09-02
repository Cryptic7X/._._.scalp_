"""
15-Minute CipherB Analysis Engine
Analyzes Heikin-Ashi candles for CipherB signals (Stage 1)
"""

import json
import logging
import yaml
import pandas as pd
import ccxt
from datetime import datetime, timedelta
from pathlib import Path
from database.signal_manager import SignalManager
from indicators.cipherb_fixed import create_cipherb_indicator
from utils.heikin_ashi import prepare_heikin_ashi_data
from alerts.two_stage_telegram import send_monitoring_alert

logger = logging.getLogger(__name__)

class CipherBAnalyzer15m:
    def __init__(self):
        self.config = self.load_config()
        self.signal_manager = SignalManager()
        self.cipher_indicator = create_cipherb_indicator(self.config)
        self.exchange = ccxt.binance()
        
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
        """Load cached market data"""
        cache_path = Path("cache/high_risk_market_data.json")
        try:
            with open(cache_path, 'r') as file:
                cache_data = json.load(file)
                return cache_data.get('coins', [])
        except Exception as e:
            logger.error(f"Error loading cached coins: {e}")
            return []
    
    def get_15m_ohlc_data(self, symbol, limit=100):
        """
        Get 15-minute OHLC data from exchange
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            limit: Number of candles to fetch
            
        Returns:
            DataFrame: OHLC data or None if error
        """
        try:
            # Fetch OHLCV data
            ohlcv = self.exchange.fetch_ohlcv(symbol, '15m', limit=limit)
            
            if not ohlcv:
                logger.warning(f"No OHLCV data for {symbol}")
                return None
            
            # Convert to DataFrame with proper column names
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.set_index('timestamp')
            
            logger.debug(f"Fetched {len(df)} 15m candles for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching 15m data for {symbol}: {e}")
            return None
    
    def analyze_symbol_for_signals(self, coin_data):
        """
        Analyze a single symbol for CipherB signals
        
        Args:
            coin_data: Dictionary containing coin information
            
        Returns:
            dict: Signal information or None
        """
        try:
            symbol = coin_data['symbol']
            
            # Get 15m OHLC data
            ohlc_df = self.get_15m_ohlc_data(symbol)
            if ohlc_df is None or ohlc_df.empty:
                return None
            
            # Convert to Heikin-Ashi
            ha_df = prepare_heikin_ashi_data(ohlc_df, symbol)
            if ha_df is None or ha_df.empty:
                return None
            
            # Analyze with CipherB using your validated indicator
            results = self.cipher_indicator.analyze_symbol(ha_df, symbol)
            if not results or not results.get('has_signal'):
                return None
            
            # Extract signal information
            signal_info = results['latest_signal']
            if not signal_info:
                return None
            
            # Add market data
            signal_data = {
                'symbol': symbol,
                'signal_direction': signal_info['signal_type'],
                'current_price': coin_data.get('current_price', 0),
                'market_cap': coin_data.get('market_cap', 0),
                'price_change_percentage_24h': coin_data.get('price_change_percentage_24h', 0),
                'wt1_value': signal_info.get('wt1'),
                'wt2_value': signal_info.get('wt2'),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"CipherB {signal_info['signal_type']} signal for {symbol}")
            return signal_data
            
        except Exception as e:
            logger.error(f"Error analyzing {coin_data.get('symbol', 'UNKNOWN')}: {e}")
            return None
    
    def process_all_coins(self, coins_data):
        """
        Process all coins for CipherB signals
        
        Args:
            coins_data: List of coin data dictionaries
            
        Returns:
            tuple: (buy_signals, sell_signals)
        """
        buy_signals = []
        sell_signals = []
        
        logger.info(f"Analyzing {len(coins_data)} coins for CipherB signals...")
        
        for coin_data in coins_data:
            try:
                signal_data = self.analyze_symbol_for_signals(coin_data)
                
                if signal_data:
                    # Store signal in database
                    success = self.signal_manager.add_signal(
                        coin_symbol=signal_data['symbol'],
                        signal_direction=signal_data['signal_direction'],
                        price_at_signal=signal_data['current_price'],
                        cipherb_value=signal_data.get('wt1_value', 0)
                    )
                    
                    if success:
                        # Categorize signal
                        if signal_data['signal_direction'] == 'BUY':
                            buy_signals.append(signal_data)
                        elif signal_data['signal_direction'] == 'SELL':
                            sell_signals.append(signal_data)
                
            except Exception as e:
                logger.error(f"Error processing coin: {e}")
                continue
        
        logger.info(f"Found {len(buy_signals)} BUY and {len(sell_signals)} SELL signals")
        return buy_signals, sell_signals
    
    def cleanup_expired_signals(self):
        """Clean up expired signals from database"""
        try:
            self.signal_manager.cleanup_expired_signals()
        except Exception as e:
            logger.error(f"Error cleaning up expired signals: {e}")
    
    def send_alerts(self, buy_signals, sell_signals):
        """
        Send Stage 1 monitoring alerts
        
        Args:
            buy_signals: List of BUY signals
            sell_signals: List of SELL signals
            
        Returns:
            bool: True if alerts sent successfully
        """
        try:
            if buy_signals or sell_signals:
                success = send_monitoring_alert(buy_signals, sell_signals)
                if success:
                    logger.info("Stage 1 monitoring alerts sent successfully")
                else:
                    logger.error("Failed to send Stage 1 monitoring alerts")
                return success
            else:
                logger.info("No signals to alert")
                return True
                
        except Exception as e:
            logger.error(f"Error sending alerts: {e}")
            return False

def analyze_15m():
    """Main function for 15-minute analysis"""
    try:
        analyzer = CipherBAnalyzer15m()
        
        # Load cached coin data
        coins_data = analyzer.load_cached_coins()
        if not coins_data:
            logger.error("No cached coin data available")
            return False
        
        # Clean up expired signals first
        analyzer.cleanup_expired_signals()
        
        # Process all coins for signals
        buy_signals, sell_signals = analyzer.process_all_coins(coins_data)
        
        # Send alerts if any signals found
        if buy_signals or sell_signals:
            success = analyzer.send_alerts(buy_signals, sell_signals)
            if not success:
                logger.error("Failed to send alerts")
                return False
        
        logger.info("15-minute analysis completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in 15-minute analysis: {e}")
        return False

if __name__ == "__main__":
    analyze_15m()
