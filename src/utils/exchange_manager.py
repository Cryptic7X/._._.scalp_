"""
Exchange Manager with Fallback Logic
Handles OHLC data fetching from multiple India-friendly exchanges
"""

import ccxt
import logging
import os
import pandas as pd
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class ExchangeManager:
    def __init__(self):
        self.exchanges = self.initialize_exchanges()
    
    def initialize_exchanges(self):
        """Initialize multiple exchange connections in priority order"""
        exchanges = {}
        
        # Primary: BingX
        try:
            bingx_api_key = os.getenv('BINGX_API_KEY')
            bingx_secret = os.getenv('BINGX_SECRET_KEY')
            
            if bingx_api_key and bingx_secret:
                exchanges['bingx'] = ccxt.bingx({
                    'apiKey': bingx_api_key,
                    'secret': bingx_secret,
                    'sandbox': False,
                    'enableRateLimit': True,
                    'timeout': 30000,
                })
                logger.info("BingX exchange initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize BingX: {e}")
        
        # Fallback: KuCoin
        try:
            kucoin_api_key = os.getenv('KUCOIN_API_KEY')
            kucoin_secret = os.getenv('KUCOIN_SECRET_KEY')
            kucoin_passphrase = os.getenv('KUCOIN_PASSPHRASE')
            
            if kucoin_api_key and kucoin_secret and kucoin_passphrase:
                exchanges['kucoin'] = ccxt.kucoin({
                    'apiKey': kucoin_api_key,
                    'secret': kucoin_secret,
                    'password': kucoin_passphrase,
                    'sandbox': False,
                    'enableRateLimit': True,
                    'timeout': 30000,
                })
                logger.info("KuCoin exchange initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize KuCoin: {e}")
        
        # Fallback: OKX (no API keys needed for public data)
        try:
            exchanges['okx'] = ccxt.okx({
                'sandbox': False,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            logger.info("OKX exchange initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize OKX: {e}")
        
        # Fallback: Bybit (no API keys needed for public data)
        try:
            exchanges['bybit'] = ccxt.bybit({
                'sandbox': False,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            logger.info("Bybit exchange initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Bybit: {e}")
        
        return exchanges
    
    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol format for different exchanges"""
        # Remove USDT and add back with proper format
        base_symbol = symbol.replace('USDT', '')
        
        if exchange_id == 'kucoin':
            return f"{base_symbol}-USDT"
        else:
            return f"{base_symbol}/USDT"
    
    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str = '15m', limit: int = 100) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data with fallback across multiple exchanges
        
        Args:
            symbol: Original symbol (e.g., 'BTCUSDT')
            timeframe: Timeframe ('15m', '5m', etc.)
            limit: Number of candles
            
        Returns:
            DataFrame with OHLC data or None if all exchanges fail
        """
        exchange_order = ['bingx', 'kucoin', 'okx', 'bybit']
        
        for exchange_id in exchange_order:
            if exchange_id not in self.exchanges:
                continue
                
            try:
                exchange = self.exchanges[exchange_id]
                normalized_symbol = self.normalize_symbol(symbol, exchange_id)
                
                logger.info(f"Trying to fetch {symbol} from {exchange_id.upper()} as {normalized_symbol}")
                
                # Fetch OHLCV data
                ohlcv = exchange.fetch_ohlcv(normalized_symbol, timeframe, limit=limit)
                
                if not ohlcv:
                    logger.warning(f"No data returned from {exchange_id.upper()} for {symbol}")
                    continue
                
                # Convert to DataFrame with proper column names
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.set_index('timestamp')
                
                logger.info(f"Successfully fetched {len(df)} candles for {symbol} from {exchange_id.upper()}")
                return df
                
            except Exception as e:
                logger.warning(f"Failed to fetch {symbol} from {exchange_id.upper()}: {str(e)}")
                continue
        
        logger.error(f"Failed to fetch {symbol} from all available exchanges")
        return None
    
    def get_available_exchanges(self) -> list:
        """Get list of available exchange IDs"""
        return list(self.exchanges.keys())

# Global instance
exchange_manager = ExchangeManager()

def fetch_ohlcv_data(symbol: str, timeframe: str = '15m', limit: int = 100) -> Optional[pd.DataFrame]:
    """Convenience function to fetch OHLCV data with fallback"""
    return exchange_manager.fetch_ohlcv_with_fallback(symbol, timeframe, limit)
