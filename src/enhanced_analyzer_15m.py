"""
Enhanced Two-Stage High-Risk 15-Minute CipherB + VWMA Analysis System
Stage 1: CipherB directional signals → Stage 2: VWMA cross confirmation
"""

import json
import os
import sys
import time
import ccxt
import pandas as pd
import yaml
import sqlite3
from datetime import datetime, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from utils.heikin_ashi import heikin_ashi
from indicators.cipherb_fixed import detect_cipherb_signals
from alerts.two_stage_telegram import send_stage1_monitoring_alert, send_stage2_execution_alert
from database.signal_manager import SignalManager

class EnhancedHighRisk15mAnalyzer:
    def __init__(self):
        self.config = self.load_configuration()
        self.exchanges = self.initialize_exchanges()
        self.signal_manager = SignalManager()
        
    def load_configuration(self):
        """Load enhanced two-stage system configuration"""
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'enhanced_config.yaml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def load_high_risk_market_data(self):
        """Load high-risk filtered market data"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ High-risk market data not found. Run data fetcher first.")
            return None
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        metadata = data.get('metadata', {})
        
        print(f"📊 Loaded {len(coins)} high-risk coins")
        print(f"🕐 Last updated: {metadata.get('last_updated', 'Unknown')}")
        
        return coins
    
    def initialize_exchanges(self):
        """Initialize exchange connections for 15m data"""
        exchanges = []
        
        # BingX (Primary)
        try:
            bingx_config = {
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 200,
                'enableRateLimit': True,
                'timeout': self.config['exchanges']['timeout'] * 1000,
            }
            
            bingx = ccxt.bingx(bingx_config)
            exchanges.append(('BingX', bingx))
            
        except Exception as e:
            print(f"⚠️ BingX initialization failed: {e}")
        
        # KuCoin (Fallback)
        try:
            kucoin_config = {
                'rateLimit': 800,
                'enableRateLimit': True,
                'timeout': self.config['exchanges']['timeout'] * 1000,
            }
            
            kucoin = ccxt.kucoin(kucoin_config)
            exchanges.append(('KuCoin', kucoin))
            
        except Exception as e:
            print(f"⚠️ KuCoin initialization failed: {e}")
        
        return exchanges
    
    def fetch_15m_data(self, symbol):
        """Fetch 15-minute OHLCV data with validation"""
        candles_required = self.config['scan']['candles_required']
        
        for exchange_name, exchange in self.exchanges:
            try:
                # Fetch 15m candles
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=candles_required)
                
                if len(ohlcv) < 100:
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Convert to IST
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if self.validate_15m_data_quality(df, symbol):
                    return df, exchange_name
                
            except Exception as e:
                continue
        
        return None, None
    
    def validate_15m_data_quality(self, df, symbol):
        """Validate 15m data quality"""
        if df.empty or len(df) < 50:
            return False
        
        if df['Close'].iloc[-1] <= 0 or df['Volume'].iloc[-1] <= 0:
            return False
        
        price_range = df['High'].max() - df['Low'].min()
        if price_range / df['Close'].mean() < 0.005:
            return False
        
        return True
    
    def calculate_vwma_21(self, df):
        """
        Calculate 21-period VWMA using regular OHLCV data
        Returns: Latest VWMA value or None if calculation fails
        """
        try:
            if len(df) < 21:
                return None
            
            close = df['Close']
            volume = df['Volume']
            
            # VWMA = sum(close * volume) / sum(volume) over 21 periods
            pv = (close * volume).rolling(window=21).sum()
            v = volume.rolling(window=21).sum()
            vwma = pv / v
            
            # Return the latest VWMA value
            latest_vwma = vwma.iloc[-1]
            
            if pd.isna(latest_vwma) or latest_vwma <= 0:
                return None
                
            return float(latest_vwma)
            
        except Exception as e:
            print(f"❌ VWMA calculation failed: {e}")
            return None
    
    def detect_new_cipherb_signals(self, coin_data):
        """
        Detect new CipherB signals and manage pending signals
        Returns: (has_new_signal, signal_data)
        """
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch 15m data
            price_df, exchange_used = self.fetch_15m_data(symbol)
            if price_df is None:
                return False, None
            
            # Convert to Heikin-Ashi for CipherB
            ha_data = heikin_ashi(price_df)
            
            # Apply CipherB indicator
            cipherb_signals = detect_cipherb_signals(ha_data, self.config['cipherb'])
            if cipherb_signals.empty:
                return False, None
            
            # Get latest signal data
            latest_signals = cipherb_signals.iloc[-1]
            latest_timestamp = cipherb_signals.index[-1]
            
            # Calculate current VWMA for reference
            current_vwma = self.calculate_vwma_21(price_df)
            if current_vwma is None:
                print(f"⚠️ {symbol}: VWMA calculation failed, skipping")
                return False, None
            
            current_price = price_df['Close'].iloc[-1]
            
            # Check for new signals
            new_signal_data = None
            
            if latest_signals['buySignal']:
                # Cancel any existing SELL signal for this symbol
                self.signal_manager.cancel_opposite_signal(symbol, 'SELL')
                
                # Check if we already have a BUY signal monitoring
                existing_signal = self.signal_manager.get_pending_signal(symbol, 'BUY')
                if not existing_signal:
                    # Store new BUY signal
                    signal_id = self.signal_manager.store_pending_signal(
                        symbol=symbol,
                        direction='BUY',
                        wt1=latest_signals['wt1'],
                        wt2=latest_signals['wt2'],
                        vwma_at_signal=current_vwma,
                        current_price=current_price,
                        exchange=exchange_used
                    )
                    
                    new_signal_data = {
                        'signal_id': signal_id,
                        'symbol': symbol,
                        'direction': 'BUY',
                        'wt1': latest_signals['wt1'],
                        'wt2': latest_signals['wt2'],
                        'vwma_target': current_vwma,
                        'current_price': current_price,
                        'exchange': exchange_used,
                        'coin_data': coin_data,
                        'timestamp': latest_timestamp
                    }
            
            elif latest_signals['sellSignal']:
                # Cancel any existing BUY signal for this symbol
                self.signal_manager.cancel_opposite_signal(symbol, 'BUY')
                
                # Check if we already have a SELL signal monitoring
                existing_signal = self.signal_manager.get_pending_signal(symbol, 'SELL')
                if not existing_signal:
                    # Store new SELL signal
                    signal_id = self.signal_manager.store_pending_signal(
                        symbol=symbol,
                        direction='SELL',
                        wt1=latest_signals['wt1'],
                        wt2=latest_signals['wt2'],
                        vwma_at_signal=current_vwma,
                        current_price=current_price,
                        exchange=exchange_used
                    )
                    
                    new_signal_data = {
                        'signal_id': signal_id,
                        'symbol': symbol,
                        'direction': 'SELL',
                        'wt1': latest_signals['wt1'],
                        'wt2': latest_signals['wt2'],
                        'vwma_target': current_vwma,
                        'current_price': current_price,
                        'exchange': exchange_used,
                        'coin_data': coin_data,
                        'timestamp': latest_timestamp
                    }
            
            return new_signal_data is not None, new_signal_data
            
        except Exception as e:
            print(f"❌ {symbol} CipherB analysis failed: {str(e)[:100]}")
            return False, None
    
    def check_vwma_crosses_for_pending_signals(self):
        """
        Check all pending signals for VWMA crosses
        Returns: List of execution_signals
        """
        execution_signals = []
        pending_signals = self.signal_manager.get_all_pending_signals()
        
        print(f"🔍 Checking {len(pending_signals)} pending signals for VWMA crosses")
        
        for pending in pending_signals:
            try:
                symbol = pending['symbol']
                direction = pending['direction']
                
                # Fetch current price data
                price_df, exchange_used = self.fetch_15m_data(symbol)
                if price_df is None:
                    print(f"⚠️ {symbol}: No price data, deleting pending signal")
                    self.signal_manager.delete_signal(pending['signal_id'])
                    continue
                
                # Calculate current VWMA
                current_vwma = self.calculate_vwma_21(price_df)
                if current_vwma is None:
                    print(f"⚠️ {symbol}: VWMA calculation failed, deleting pending signal")
                    self.signal_manager.delete_signal(pending['signal_id'])
                    continue
                
                current_close = price_df['Close'].iloc[-1]
                
                # Check for VWMA cross
                cross_confirmed = False
                
                if direction == 'BUY' and current_close > current_vwma:
                    cross_confirmed = True
                    print(f"🚀 {symbol} BUY: Price {current_close:.6f} crossed above VWMA {current_vwma:.6f}")
                
                elif direction == 'SELL' and current_close < current_vwma:
                    cross_confirmed = True
                    print(f"🔻 {symbol} SELL: Price {current_close:.6f} crossed below VWMA {current_vwma:.6f}")
                
                if cross_confirmed:
                    # Calculate signal age
                    created_at = datetime.fromisoformat(pending['created_at'])
                    signal_age = datetime.now() - created_at
                    
                    execution_signal = {
                        'signal_id': pending['signal_id'],
                        'symbol': symbol,
                        'direction': direction,
                        'wt1': pending['wt1'],
                        'wt2': pending['wt2'],
                        'entry_price': current_close,
                        'vwma_value': current_vwma,
                        'exchange': exchange_used,
                        'signal_age': signal_age,
                        'created_at': pending['created_at']
                    }
                    
                    execution_signals.append(execution_signal)
                    
                    # Mark as executed and delete
                    self.signal_manager.delete_signal(pending['signal_id'])
                
            except Exception as e:
                print(f"❌ Error checking {pending['symbol']}: {e}")
                # Delete problematic signal
                self.signal_manager.delete_signal(pending['signal_id'])
                continue
        
        return execution_signals
    
    def run_enhanced_analysis(self):
        """Execute enhanced two-stage high-risk analysis"""
        print(f"\n" + "="*80)
        print("🔔 ENHANCED HIGH-RISK 15M CIPHERB + VWMA ANALYSIS")
        print("="*80)
        print(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏱️ System: Two-Stage CipherB + VWMA Confirmation")
        print(f"📊 Stage 1: CipherB Signal Detection")
        print(f"🎯 Stage 2: VWMA Cross Confirmation")
        
        # Load high-risk market data
        coins = self.load_high_risk_market_data()
        if not coins:
            return
        
        print(f"📊 Available exchanges: {[name for name, _ in self.exchanges]}")
        print(f"📈 High-risk coins to analyze: {len(coins)}")
        
        # STAGE 1: CipherB Signal Detection
        print(f"\n🔍 STAGE 1: DETECTING NEW CIPHERB SIGNALS")
        print("-" * 50)
        
        new_signals = []
        batch_size = self.config['alerts']['batch_size']
        
        for i in range(0, len(coins), batch_size):
            batch = coins[i:i+batch_size]
            batch_new_signals = 0
            
            print(f"🔄 Analyzing batch {i//batch_size + 1}: coins {i+1}-{min(i+batch_size, len(coins))}")
            
            for coin in batch:
                has_signal, signal_data = self.detect_new_cipherb_signals(coin)
                if has_signal and signal_data:
                    new_signals.append(signal_data)
                    batch_new_signals += 1
                    print(f"   🆕 {signal_data['symbol']} {signal_data['direction']} - WT1:{signal_data['wt1']:.1f}")
                
                time.sleep(self.config['exchanges']['rate_limit'])
            
            print(f"   ✅ Batch {i//batch_size + 1}: {batch_new_signals} new signals")
            
            if i + batch_size < len(coins):
                time.sleep(1)
        
        # Send Stage 1 alerts for new signals
        if new_signals:
            print(f"\n📨 Sending {len(new_signals)} Stage 1 monitoring alerts...")
            for signal in new_signals:
                send_stage1_monitoring_alert(signal)
        
        # STAGE 2: VWMA Cross Monitoring
        print(f"\n🎯 STAGE 2: CHECKING VWMA CROSSES FOR PENDING SIGNALS")
        print("-" * 60)
        
        execution_signals = self.check_vwma_crosses_for_pending_signals()
        
        # Send Stage 2 execution alerts
        if execution_signals:
            print(f"\n🚀 Sending {len(execution_signals)} Stage 2 execution alerts...")
            for signal in execution_signals:
                send_stage2_execution_alert(signal)
        else:
            print("📊 No VWMA crosses detected for pending signals")
        
        # Cleanup expired signals
        expired_count = self.signal_manager.cleanup_expired_signals(
            hours=self.config['signal_management']['max_monitoring_hours']
        )
        if expired_count > 0:
            print(f"🧹 Cleaned up {expired_count} expired signals (>{self.config['signal_management']['max_monitoring_hours']}h)")
        
        # Final summary
        print(f"\n" + "="*80)
        print("🎯 ENHANCED ANALYSIS COMPLETE")
        print("="*80)
        print(f"🆕 Stage 1 - New CipherB signals: {len(new_signals)}")
        print(f"🚀 Stage 2 - VWMA execution alerts: {len(execution_signals)}")
        print(f"⏳ Currently monitoring: {len(self.signal_manager.get_all_pending_signals())} signals")
        print(f"⏰ Next analysis: {(datetime.now() + timedelta(minutes=15)).strftime('%H:%M:%S IST')}")
        print("="*80)

def main():
    analyzer = EnhancedHighRisk15mAnalyzer()
    analyzer.run_enhanced_analysis()

if __name__ == '__main__':
    main()
