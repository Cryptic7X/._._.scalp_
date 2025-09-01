"""
Enhanced High-Risk Data Fetcher for Two-Stage System
Fetches and caches market data for CipherB + VWMA analysis
"""

import requests
import json
import os
import time
from datetime import datetime

class EnhancedHighRiskDataFetcher:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        
        # Add API key if available
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            self.session.headers.update({'x-cg-demo-api-key': api_key})
    
    def fetch_high_risk_coins(self):
        """
        Fetch high-risk coins with enhanced filtering
        Market cap >= 100M USD, Volume >= 20M USD
        """
        print("🔄 Fetching high-risk crypto market data...")
        
        all_coins = []
        
        # Fetch 4 pages to get ~1000 coins
        for page in range(1, 5):
            try:
                print(f"📄 Fetching page {page}/4...")
                
                url = f"{self.base_url}/coins/markets"
                params = {
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc',
                    'per_page': 250,
                    'page': page,
                    'sparkline': False,
                    'price_change_percentage': '24h'
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                page_data = response.json()
                all_coins.extend(page_data)
                
                time.sleep(1.2)  # Rate limit compliance
                
            except Exception as e:
                print(f"❌ Error fetching page {page}: {e}")
                continue
        
        print(f"📊 Total coins fetched: {len(all_coins)}")
        
        # Apply high-risk filters
        filtered_coins = self.apply_high_risk_filters(all_coins)
        
        # Cache the data
        self.cache_market_data(filtered_coins)
        
        return filtered_coins
    
    def apply_high_risk_filters(self, coins):
        """
        Apply filters for high-risk trading:
        - Market cap >= $100M
        - 24h volume >= $20M
        - Valid price data
        - Exclude stablecoins
        """
        print("🔍 Applying high-risk filters...")
        
        # Load blocked coins
        blocked_coins = self.load_blocked_coins()
        
        filtered = []
        
        for coin in coins:
            try:
                # Basic data validation
                if not coin.get('symbol') or not coin.get('current_price'):
                    continue
                
                symbol = coin['symbol'].upper()
                
                # Skip blocked coins
                if symbol in blocked_coins:
                    continue
                
                market_cap = coin.get('market_cap') or 0
                volume_24h = coin.get('total_volume') or 0
                price = coin.get('current_price') or 0
                
                # High-risk filters
                if market_cap >= 100_000_000:  # >= $100M market cap
                    if volume_24h >= 20_000_000:  # >= $20M volume
                        if price > 0:  # Valid price
                            # Additional quality checks
                            if coin.get('price_change_percentage_24h') is not None:
                                filtered.append({
                                    'id': coin['id'],
                                    'symbol': symbol,
                                    'name': coin['name'],
                                    'current_price': price,
                                    'market_cap': market_cap,
                                    'market_cap_rank': coin.get('market_cap_rank'),
                                    'total_volume': volume_24h,
                                    'price_change_percentage_24h': coin.get('price_change_percentage_24h', 0),
                                    'circulating_supply': coin.get('circulating_supply'),
                                    'last_updated': coin.get('last_updated')
                                })
            
            except Exception as e:
                continue
        
        print(f"✅ High-risk coins after filtering: {len(filtered)}")
        print(f"📊 Market cap range: ${filtered[0]['market_cap']/1e9:.1f}B - ${filtered[-1]['market_cap']/1e9:.1f}B")
        print(f"📈 Volume range: ${min(c['total_volume'] for c in filtered)/1e6:.1f}M - ${max(c['total_volume'] for c in filtered)/1e6:.1f}M")
        
        return filtered
    
    def load_blocked_coins(self):
        """Load blocked coins from configuration"""
        blocked_coins = set()
        
        # From config file
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'enhanced_config.yaml')
        if os.path.exists(config_path):
            try:
                import yaml
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                
                blocked_list = config.get('blocked_coins', [])
                blocked_coins.update([coin.upper() for coin in blocked_list])
                
            except Exception as e:
                print(f"⚠️ Error loading blocked coins from config: {e}")
        
        # Default blocked coins
        default_blocked = {
            'USDT', 'USDC', 'BUSD', 'DAI', 'TUSD', 'FDUSD', 'USDD', 'FRAX',
            'WBTC', 'WETH', 'WBNB', 'WMATIC', 'WAVAX'
        }
        blocked_coins.update(default_blocked)
        
        print(f"🚫 Blocked coins: {len(blocked_coins)} tokens")
        
        return blocked_coins
    
    def cache_market_data(self, coins):
        """Cache market data for analyzer"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_data = {
            'coins': coins,
            'metadata': {
                'last_updated': datetime.now().isoformat(),
                'total_coins': len(coins),
                'fetch_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                'system': 'Enhanced CipherB + VWMA High-Risk System v2.0'
            }
        }
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        print(f"💾 Cached {len(coins)} coins to {cache_file}")
        
        return cache_file
    
    def get_data_summary(self):
        """Generate data summary for logging"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            return "No cached data available"
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            coins = data.get('coins', [])
            metadata = data.get('metadata', {})
            
            if not coins:
                return "Empty dataset"
            
            # Calculate statistics
            total_market_cap = sum(coin['market_cap'] for coin in coins) / 1e12  # In trillions
            total_volume = sum(coin['total_volume'] for coin in coins) / 1e9  # In billions
            
            avg_volatility = sum(abs(coin.get('price_change_percentage_24h', 0)) for coin in coins) / len(coins)
            
            summary = f"""
📊 HIGH-RISK MARKET DATA SUMMARY
{'='*50}
🪙 Total coins: {len(coins)}
💰 Combined market cap: ${total_market_cap:.2f}T
📈 Combined 24h volume: ${total_volume:.2f}B
📊 Average 24h volatility: {avg_volatility:.2f}%
🕐 Last updated: {metadata.get('last_updated', 'Unknown')}
🎯 System: Enhanced CipherB + VWMA v2.0

Top 10 by Market Cap:"""
            
            for i, coin in enumerate(coins[:10], 1):
                price = coin['current_price']
                change = coin.get('price_change_percentage_24h', 0)
                volume = coin['total_volume'] / 1e6
                
                if price < 0.001:
                    price_fmt = f"${price:.8f}"
                elif price < 1:
                    price_fmt = f"${price:.4f}"
                else:
                    price_fmt = f"${price:.2f}"
                
                summary += f"\n{i:2d}. {coin['symbol']:8s} {price_fmt:>12s} {change:+6.2f}% Vol: ${volume:6.0f}M"
            
            return summary
            
        except Exception as e:
            return f"Error generating summary: {e}"

def main():
    """Main execution function"""
    print(f"\n{'='*60}")
    print("🚀 ENHANCED HIGH-RISK DATA FETCHER v2.0")
    print("🎯 CipherB + VWMA Confirmation System")
    print(f"{'='*60}")
    
    fetcher = EnhancedHighRiskDataFetcher()
    
    try:
        # Fetch and filter high-risk coins
        coins = fetcher.fetch_high_risk_coins()
        
        # Generate and print summary
        summary = fetcher.get_data_summary()
        print(summary)
        
        print(f"\n{'='*60}")
        print("✅ DATA FETCH COMPLETED SUCCESSFULLY")
        print("🔄 Ready for enhanced CipherB + VWMA analysis")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        raise

if __name__ == '__main__':
    main()
