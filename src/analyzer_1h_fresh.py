#!/usr/bin/env python3
"""
Fresh Signal 1h Analyzer with Blocked Coins Support
- Analyzes 1-hour candles for CipherB signals
- Only alerts on fresh signals (within 1 hour + buffer)
- Skips coins in blocked_coins.txt file
- Exact Pine Script replication for maximum accuracy
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from alerts.telegram_batch import send_consolidated_alert
from alerts.deduplication_fresh import FreshSignalDeduplicator
from indicators.cipherb_exact import detect_exact_cipherb_signals

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class Fresh1hAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        # Use 65 minutes to catch signals from the full 1-hour candle + buffer
        self.deduplicator = FreshSignalDeduplicator(freshness_minutes=65)
        self.exchanges = self.init_exchanges()
        self.blocked_coins = self.load_blocked_coins()
        self.market_data = self.load_market_data()
    
    def load_blocked_coins(self):
        """
        Load blocked coins from blocked_coins.txt
        Returns set of uppercase coin symbols to block
        """
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins")
            if blocked_coins:
                print(f"   Blocked: {', '.join(sorted(list(blocked_coins)[:10]))}")
                if len(blocked_coins) > 10:
                    print(f"   ... and {len(blocked_coins) - 10} more")
                    
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
        
        return blocked_coins
    
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_market_data(self):
        """Load market data and filter out blocked coins"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        all_coins = data.get('coins', [])
        
        # Separate major coins for priority processing
        major_coins_list = ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOT', 'MATIC', 'LINK', 'AVAX', 'UNI']
        priority_coins = []
        regular_coins = []
        blocked_count = 0
        
        for coin in all_coins:
            symbol = coin.get('symbol', '').upper()
            if symbol in self.blocked_coins:
                blocked_count += 1
                continue
                
            if symbol in major_coins_list:
                priority_coins.append(coin)
            else:
                regular_coins.append(coin)
        
        # Sort priority coins by market cap
        priority_coins.sort(key=lambda x: x.get('market_cap', 0), reverse=True)
        
        # Combine with priority coins first
        filtered_coins = priority_coins + regular_coins
        
        print(f"📊 Market data: {len(all_coins)} total, {blocked_count} blocked, {len(filtered_coins)} to analyze")
        print(f"🎯 Priority coins: {len(priority_coins)} major coins processed first")
        
        return filtered_coins
    
    def init_exchanges(self):
        exchanges = []
        
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX failed: {e}")
        
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin failed: {e}")
        
        return exchanges
    
    def is_coin_blocked(self, symbol):
        """Check if coin is in blocked list"""
        return symbol.upper() in self.blocked_coins
    
    def fetch_1h_ohlcv(self, symbol):
        """Fetch 1-hour OHLCV data with timestamps"""
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '1h', limit=200)
                
                if len(ohlcv) < 100:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Keep UTC timestamps for freshness checking
                df['utc_timestamp'] = df.index
                
                # Convert index to IST for display
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) > 50 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                    
            except Exception as e:
                continue
        
        return None, None
    
    def analyze_coin_fresh_signals(self, coin_data):
        """
        Analyze for FRESH SIGNALS ONLY - 1h timeframe
        Includes blocked coin check for safety
        """
        symbol = coin_data.get('symbol', '').upper()
        
        # Double-check blocked coins (should already be filtered)
        if self.is_coin_blocked(symbol):
            print(f"🚫 Skipping blocked coin: {symbol}")
            return None
        
        try:
            # Fetch 1h data with timestamps
            price_df, exchange_used = self.fetch_1h_ohlcv(symbol)
            
            if price_df is None or len(price_df) < 50:
                return None
            
            # Apply exact CipherB detection
            signals_df = detect_exact_cipherb_signals(price_df, self.config['cipherb'])
            
            if signals_df.empty:
                return None
            
            # Only check the MOST RECENT CLOSED candle
            latest_signal = signals_df.iloc[-1]
            signal_timestamp_utc = price_df['utc_timestamp'].iloc[-1]
            signal_timestamp_ist = signals_df.index[-1]
            
            # Debug output for verification
            current_time = datetime.utcnow()
            time_since_signal = current_time - signal_timestamp_utc.to_pydatetime()
            
            print(f"🔍 {symbol} - Signal age: {time_since_signal.total_seconds():.0f}s")
            print(f"   Signal time (IST): {signal_timestamp_ist.strftime('%H:%M:%S')}")
            print(f"   BUY: {latest_signal['buySignal']} | SELL: {latest_signal['sellSignal']}")
            
            # Check for FRESH BUY signal
            if latest_signal['buySignal']:
                if self.deduplicator.is_signal_fresh_and_new(symbol, 'BUY', signal_timestamp_utc):
                    return {
                        'symbol': symbol,
                        'signal_type': 'BUY',
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data.get('current_price', 0),
                        'change_24h': coin_data.get('price_change_percentage_24h', 0),
                        'market_cap': coin_data.get('market_cap', 0),
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp_ist,
                        'signal_age_seconds': time_since_signal.total_seconds(),
                        'coin_data': coin_data
                    }
            
            # Check for FRESH SELL signal
            if latest_signal['sellSignal']:
                if self.deduplicator.is_signal_fresh_and_new(symbol, 'SELL', signal_timestamp_utc):
                    return {
                        'symbol': symbol,
                        'signal_type': 'SELL',
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data.get('current_price', 0),
                        'change_24h': coin_data.get('price_change_percentage_24h', 0),
                        'market_cap': coin_data.get('market_cap', 0),
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp_ist,
                        'signal_age_seconds': time_since_signal.total_seconds(),
                        'coin_data': coin_data
                    }
            
            return None
            
        except Exception as e:
            print(f"❌ {symbol} analysis failed: {str(e)[:100]}")
            return None
    
    def run_fresh_analysis(self):
        """
        Run 1h analysis for FRESH SIGNALS ONLY
        Now includes blocked coins filtering and priority processing
        """
        ist_current = get_ist_time()
        
        print("="*80)
        print("🎯 FRESH SIGNAL 1H ANALYSIS (WITH BLOCKED COINS)")
        print("="*80)
        print(f"🕐 Analysis Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏰ Timeframe: 1-hour candles (close at :30 IST)")
        print(f"✅ Only fresh signals (within 1 hour)")
        print(f"🚫 Blocks duplicate & stale signals + blocked coins")
        print(f"🔍 Coins to analyze: {len(self.market_data)} (after blocking)")
        
        if not self.market_data:
            print("❌ No market data available (all coins may be blocked)")
            return
        
        # Clean up old signal records first
        self.deduplicator.cleanup_old_signals()
        
        # Collect FRESH signals only (blocked coins already filtered, major coins prioritized)
        fresh_signals = []
        batch_size = 20
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"\n🔄 Processing batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin_fresh_signals(coin)
                if signal_result:
                    fresh_signals.append(signal_result)
                    age_s = signal_result['signal_age_seconds']
                    print(f"🚨 {signal_result['signal_type']}: {signal_result['symbol']} ({age_s:.0f}s ago)")
                
                total_analyzed += 1
                time.sleep(0.3)  # Rate limiting
        
        # Send consolidated alert with FRESH signals only
        if fresh_signals:
            success = send_consolidated_alert(fresh_signals, timeframe="1h")
            if success:
                avg_age = sum(s['signal_age_seconds'] for s in fresh_signals) / len(fresh_signals)
                print(f"\n✅ SENT 1 FRESH 1H SIGNAL ALERT")
                print(f"   Signals: {len(fresh_signals)}")
                print(f"   Average age: {avg_age:.0f} seconds")
            else:
                print(f"\n❌ Failed to send fresh signal alert")
        else:
            print(f"\n📊 No fresh signals detected")
        
        print(f"\n" + "="*80)
        print("🎯 FRESH 1H SIGNAL ANALYSIS COMPLETE")
        print("="*80)
        print(f"📊 Total analyzed: {total_analyzed}")
        print(f"🚨 Fresh signals: {len(fresh_signals)}")
        print(f"🚫 Blocked coins: {len(self.blocked_coins)}")
        print(f"📱 Alert sent: {'Yes' if fresh_signals else 'No'}")
        print(f"⏰ Next analysis: 1 hour (at :32 IST)")
        print("="*80)

if __name__ == '__main__':
    analyzer = Fresh1hAnalyzer()
    analyzer.run_fresh_analysis()
