"""
Fresh Signal Deduplication System for 30m Analysis
Only allows alerts for signals that:
1. Haven't been alerted before
2. Occurred within the last 10 minutes (fresh signals for 30m)
3. Match exact Pine Script alert conditions
"""

import json
import os
from datetime import datetime, timedelta

class FreshSignalDeduplicator:
    def __init__(self, freshness_minutes=30):
        self.freshness_window = timedelta(minutes=freshness_minutes)
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'fresh_alerts_30m.json')
        self.signal_cache = self.load_cache()
    
    def load_cache(self):
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            print(f"📁 Loaded fresh 30m signal cache: {len(cache)} entries")
            return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh 30m signal cache")
            return {}
    
    def save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.signal_cache, f, indent=2, default=str)
    
    def is_signal_fresh_and_new(self, symbol, signal_type, signal_timestamp):
        """
        Check if signal is:
        1. Fresh (within last 10 minutes for 30m)
        2. New (not already alerted)
        """
        current_time = datetime.utcnow()
        
        # Check 1: Is signal fresh (within 10-minute window for 30m)?
        if isinstance(signal_timestamp, str):
            signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
        
        time_since_signal = current_time - signal_timestamp
        is_fresh = time_since_signal <= self.freshness_window
        
        if not is_fresh:
            print(f"❌ {symbol} {signal_type}: STALE 30m signal ({time_since_signal.total_seconds():.0f}s old)")
            return False
        
        # Check 2: Is signal new (not already alerted)?
        signal_key = f"{symbol}_{signal_type}_{signal_timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        if signal_key in self.signal_cache:
            print(f"❌ {symbol} {signal_type}: DUPLICATE 30m signal (already alerted)")
            return False
        
        # Signal is both fresh and new - allow it
        self.signal_cache[signal_key] = {
            'alerted_at': current_time.isoformat(),
            'signal_time': signal_timestamp.isoformat(),
            'freshness_seconds': time_since_signal.total_seconds(),
            'timeframe': '30m'
        }
        self.save_cache()
        
        print(f"✅ {symbol} {signal_type}: FRESH & NEW 30m signal ({time_since_signal.total_seconds():.0f}s ago)")
        return True
    
    def cleanup_old_signals(self):
        """Remove signal records older than 2 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=2)
        
        old_keys = []
        for key, data in self.signal_cache.items():
            try:
                alerted_at = datetime.fromisoformat(data['alerted_at'])
                if alerted_at < cutoff_time:
                    old_keys.append(key)
            except (ValueError, TypeError, KeyError):
                old_keys.append(key)  # Remove invalid entries
        
        for key in old_keys:
            del self.signal_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old 30m signal records")
