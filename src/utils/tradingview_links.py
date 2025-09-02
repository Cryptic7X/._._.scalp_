"""
TradingView Chart Link Generator
Creates properly formatted TradingView chart URLs
"""

import logging
import yaml
from pathlib import Path
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class TradingViewLinks:
    def __init__(self):
        self.config = self.load_config()
        self.base_url = self.config['tradingview']['base_url']
        self.styles = self.config['tradingview']['style']
    
    def load_config(self):
        """Load configuration from YAML file"""
        config_path = Path("config/enhanced_config.yaml")
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def generate_heikin_ashi_link(self, symbol, timeframe='15'):
        """
        Generate TradingView link for Heikin-Ashi chart
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframe: Chart timeframe (default '15' for 15m)
            
        Returns:
            str: Complete TradingView chart URL
        """
        try:
            # Clean and format symbol
            clean_symbol = symbol.upper()
            
            # Build URL parameters
            params = {
                'symbol': f'BINANCE:{clean_symbol}',
                'interval': timeframe,
                'style': self.styles['heikin_ashi']
            }
            
            # Construct full URL
            url = f"{self.base_url}?{urlencode(params)}"
            
            logger.debug(f"Generated Heikin-Ashi link for {symbol}: {url}")
            return url
            
        except Exception as e:
            logger.error(f"Error generating Heikin-Ashi link for {symbol}: {e}")
            return self.get_fallback_link(symbol, timeframe)
    
    def generate_standard_link(self, symbol, timeframe='5'):
        """
        Generate TradingView link for standard candle chart
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframe: Chart timeframe (default '5' for 5m)
            
        Returns:
            str: Complete TradingView chart URL
        """
        try:
            # Clean and format symbol
            clean_symbol = symbol.upper()
            
            # Build URL parameters
            params = {
                'symbol': f'BINANCE:{clean_symbol}',
                'interval': timeframe,
                'style': self.styles['standard']
            }
            
            # Construct full URL
            url = f"{self.base_url}?{urlencode(params)}"
            
            logger.debug(f"Generated standard link for {symbol}: {url}")
            return url
            
        except Exception as e:
            logger.error(f"Error generating standard link for {symbol}: {e}")
            return self.get_fallback_link(symbol, timeframe)
    
    def get_fallback_link(self, symbol, timeframe='15'):
        """
        Generate fallback link if main generation fails
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            str: Basic TradingView URL
        """
        try:
            clean_symbol = symbol.upper()
            return f"{self.base_url}?symbol=BINANCE:{clean_symbol}&interval={timeframe}"
        except:
            return f"{self.base_url}?symbol=BINANCE:BTCUSDT&interval=15"
    
    def validate_symbol(self, symbol):
        """
        Validate and clean trading symbol
        
        Args:
            symbol: Raw trading symbol
            
        Returns:
            str: Cleaned symbol or None if invalid
        """
        try:
            if not symbol or not isinstance(symbol, str):
                return None
            
            # Clean the symbol
            clean_symbol = symbol.upper().strip()
            
            # Basic validation - should end with USDT
            if not clean_symbol.endswith('USDT'):
                logger.warning(f"Symbol {symbol} doesn't end with USDT")
                return None
            
            # Should be reasonable length
            if len(clean_symbol) < 5 or len(clean_symbol) > 12:
                logger.warning(f"Symbol {symbol} has unusual length")
                return None
            
            return clean_symbol
            
        except Exception as e:
            logger.error(f"Error validating symbol {symbol}: {e}")
            return None
    
    def generate_chart_links(self, symbol):
        """
        Generate both 15m Heikin-Ashi and 5m standard chart links
        
        Args:
            symbol: Trading symbol
            
        Returns:
            dict: Dictionary with 'ha_15m' and 'standard_5m' links
        """
        try:
            validated_symbol = self.validate_symbol(symbol)
            if not validated_symbol:
                logger.error(f"Invalid symbol: {symbol}")
                return {
                    'ha_15m': self.get_fallback_link(symbol, '15'),
                    'standard_5m': self.get_fallback_link(symbol, '5')
                }
            
            links = {
                'ha_15m': self.generate_heikin_ashi_link(validated_symbol, '15'),
                'standard_5m': self.generate_standard_link(validated_symbol, '5')
            }
            
            logger.debug(f"Generated chart links for {symbol}")
            return links
            
        except Exception as e:
            logger.error(f"Error generating chart links for {symbol}: {e}")
            return {
                'ha_15m': self.get_fallback_link(symbol, '15'),
                'standard_5m': self.get_fallback_link(symbol, '5')
            }

# Global instance for easy import
tv_links = TradingViewLinks()

def get_heikin_ashi_link(symbol, timeframe='15'):
    """Convenience function for Heikin-Ashi link"""
    return tv_links.generate_heikin_ashi_link(symbol, timeframe)

def get_standard_link(symbol, timeframe='5'):
    """Convenience function for standard chart link"""
    return tv_links.generate_standard_link(symbol, timeframe)

def get_chart_links(symbol):
    """Convenience function for both chart links"""
    return tv_links.generate_chart_links(symbol)

if __name__ == "__main__":
    # Test the link generation
    test_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    
    for symbol in test_symbols:
        links = get_chart_links(symbol)
        print(f"\n{symbol}:")
        print(f"  15m HA: {links['ha_15m']}")
        print(f"  5m STD: {links['standard_5m']}")
