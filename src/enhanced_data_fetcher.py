"""
Enhanced Data Fetcher for High-Risk Crypto Scanner
Fetches market data from CoinGecko API and caches for analysis
"""

import json
import logging
import os
import requests
import time
import yaml
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self):
        self.config = self.load_config()
        self.blocked_coins = self.load_blocked_coins()
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        
    def load_config(self):
        """Load configuration from YAML file"""
        config_path = Path("config/enhanced_config.yaml")
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise

    def load_blocked_coins(self):
        """Load blocked coins list"""
        blocked_path = Path("config/blocked_coins.txt")
        blocked_coins = set()
        
        if blocked_path.exists():
            try:
                with open(blocked_path, 'r') as file:
                    for line in file:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            blocked_coins.add(line.upper())
            except Exception as e:
                logger.warning(f"Error reading blocked coins file: {e}")
        
        return blocked_coins

    def get_market_data(self):
        """Fetch market data from CoinGecko API"""
        base_url = self.config['coingecko']['base_url']
        api_key = os.getenv('COINGECKO_API_KEY', '')
        
        # Parameters for market data
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 250,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h'
        }
        
        headers = {}
        if api_key:
            headers['x-cg-demo-api-key'] = api_key
        
        url = f"{base_url}/coins/markets"
        
        try:
            logger.info("Fetching market data from CoinGecko...")
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            market_data = response.json()
            logger.info(f"Fetched data for {len(market_data)} coins")
            
            return market_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching market data: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing market data JSON: {e}")
            raise

    def filter_high_risk_coins(self, market_data):
        """Filter coins based on market cap and volume criteria"""
        min_market_cap = self.config['coin_filters']['min_market_cap']
        min_24h_volume = self.config['coin_filters']['min_24h_volume']
        
        filtered_coins = []
        
        for coin in market_data:
            try:
                # Extract relevant data
                symbol = coin.get('symbol', '').upper() + 'USDT'
                market_cap = coin.get('market_cap', 0) or 0
                volume_24h = coin.get('total_volume', 0) or 0
                
                # Skip if blocked
                if symbol in self.blocked_coins:
                    continue
                
                # Apply filters
                if (market_cap >= min_market_cap and 
                    volume_24h >= min_24h_volume):
                    
                    filtered_coin = {
                        'symbol': symbol,
                        'name': coin.get('name', ''),
                        'current_price': coin.get('current_price', 0),
                        'market_cap': market_cap,
                        'total_volume': volume_24h,
                        'price_change_percentage_24h': coin.get('price_change_percentage_24h', 0),
                        'last_updated': coin.get('last_updated', '')
                    }
                    
                    filtered_coins.append(filtered_coin)
                    
            except Exception as e:
                logger.warning(f"Error processing coin data: {e}")
                continue
        
        logger.info(f"Filtered to {len(filtered_coins)} high-risk coins")
        return filtered_coins

    def cache_market_data(self, filtered_coins):
        """Save filtered market data to cache"""
        cache_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_coins': len(filtered_coins),
            'coins': filtered_coins
        }
        
        cache_file = self.cache_dir / "high_risk_market_data.json"
        
        try:
            with open(cache_file, 'w') as file:
                json.dump(cache_data, file, indent=2)
            
            logger.info(f"Market data cached to {cache_file}")
            
        except Exception as e:
            logger.error(f"Error caching market data: {e}")
            raise

    def get_cached_data(self):
        """Load cached market data"""
        cache_file = self.cache_dir / "high_risk_market_data.json"
        
        if not cache_file.exists():
            logger.warning("No cached market data found")
            return None
        
        try:
            with open(cache_file, 'r') as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"Error loading cached data: {e}")
            return None

    def is_cache_fresh(self, max_age_hours=6):
        """Check if cached data is fresh enough"""
        cached_data = self.get_cached_data()
        
        if not cached_data:
            return False
        
        try:
            cache_time = datetime.fromisoformat(cached_data['timestamp'])
            age = datetime.utcnow() - cache_time
            
            return age < timedelta(hours=max_age_hours)
            
        except Exception as e:
            logger.error(f"Error checking cache freshness: {e}")
            return False

def fetch_market_data():
    """Main function to fetch and cache market data"""
    try:
        fetcher = DataFetcher()
        
        # Get fresh market data
        market_data = fetcher.get_market_data()
        
        # Filter for high-risk coins
        filtered_coins = fetcher.filter_high_risk_coins(market_data)
        
        # Cache the results
        fetcher.cache_market_data(filtered_coins)
        
        logger.info("Market data fetch and cache completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to fetch market data: {e}")
        return False

if __name__ == "__main__":
    fetch_market_data()
