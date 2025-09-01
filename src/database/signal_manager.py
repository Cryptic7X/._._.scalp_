"""
Signal Manager for Two-Stage CipherB + VWMA Confirmation System
Handles persistent storage and retrieval of pending signals
"""

import sqlite3
import json
from datetime import datetime, timedelta
import os

class SignalManager:
    def __init__(self, db_path=None):
        if db_path is None:
            cache_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'cache')
            os.makedirs(cache_dir, exist_ok=True)
            self.db_path = os.path.join(cache_dir, 'pending_signals.db')
        else:
            self.db_path = db_path
        
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database for signal storage"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create pending_signals table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pending_signals (
                        signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        wt1 REAL NOT NULL,
                        wt2 REAL NOT NULL,
                        vwma_at_signal REAL NOT NULL,
                        current_price REAL NOT NULL,
                        exchange TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        status TEXT DEFAULT 'MONITORING'
                    )
                ''')
                
                # Create index for faster lookups
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_symbol_direction 
                    ON pending_signals(symbol, direction)
                ''')
                
                # Create index for expiry cleanup
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_expires_at 
                    ON pending_signals(expires_at)
                ''')
                
                conn.commit()
                print("✅ Signal database initialized successfully")
                
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
    
    def store_pending_signal(self, symbol, direction, wt1, wt2, vwma_at_signal, current_price, exchange):
        """
        Store a new pending signal
        Returns: signal_id if successful, None if failed
        """
        try:
            created_at = datetime.now().isoformat()
            expires_at = (datetime.now() + timedelta(hours=12)).isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO pending_signals 
                    (symbol, direction, wt1, wt2, vwma_at_signal, current_price, exchange, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, direction, wt1, wt2, vwma_at_signal, current_price, exchange, created_at, expires_at))
                
                signal_id = cursor.lastrowid
                conn.commit()
                
                print(f"💾 Stored pending {direction} signal for {symbol} (ID: {signal_id})")
                return signal_id
                
        except Exception as e:
            print(f"❌ Failed to store signal for {symbol}: {e}")
            return None
    
    def get_pending_signal(self, symbol, direction):
        """
        Get existing pending signal for symbol and direction
        Returns: signal dict or None
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row  # Enable dict-like access
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM pending_signals 
                    WHERE symbol = ? AND direction = ? AND status = 'MONITORING'
                ''', (symbol, direction))
                
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            print(f"❌ Failed to get pending signal for {symbol}: {e}")
            return None
    
    def get_all_pending_signals(self):
        """
        Get all pending signals
        Returns: List of signal dicts
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM pending_signals 
                    WHERE status = 'MONITORING'
                    ORDER BY created_at ASC
                ''')
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            print(f"❌ Failed to get all pending signals: {e}")
            return []
    
    def cancel_opposite_signal(self, symbol, direction):
        """
        Cancel opposite direction signal for the same symbol
        E.g., if new BUY signal comes, cancel existing SELL signal
        """
        try:
            opposite_direction = 'SELL' if direction == 'BUY' else 'BUY'
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if opposite signal exists
                cursor.execute('''
                    SELECT signal_id FROM pending_signals 
                    WHERE symbol = ? AND direction = ? AND status = 'MONITORING'
                ''', (symbol, opposite_direction))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Delete the opposite signal
                    cursor.execute('''
                        DELETE FROM pending_signals 
                        WHERE symbol = ? AND direction = ? AND status = 'MONITORING'
                    ''', (symbol, opposite_direction))
                    
                    conn.commit()
                    print(f"🔄 Cancelled {opposite_direction} signal for {symbol} (new {direction} signal)")
                    return True
                
                return False
                
        except Exception as e:
            print(f"❌ Failed to cancel opposite signal for {symbol}: {e}")
            return False
    
    def delete_signal(self, signal_id):
        """
        Delete a specific signal by ID
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('DELETE FROM pending_signals WHERE signal_id = ?', (signal_id,))
                conn.commit()
                
                if cursor.rowcount > 0:
                    print(f"🗑️ Deleted signal ID {signal_id}")
                    return True
                else:
                    print(f"⚠️ Signal ID {signal_id} not found")
                    return False
                    
        except Exception as e:
            print(f"❌ Failed to delete signal {signal_id}: {e}")
            return False
    
    def cleanup_expired_signals(self, hours=12):
        """
        Clean up signals older than specified hours
        Returns: Number of signals cleaned up
        """
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # First, get count of signals to be deleted
                cursor.execute('''
                    SELECT COUNT(*) FROM pending_signals 
                    WHERE created_at < ? AND status = 'MONITORING'
                ''', (cutoff_time,))
                
                count = cursor.fetchone()[0]
                
                if count > 0:
                    # Delete expired signals
                    cursor.execute('''
                        DELETE FROM pending_signals 
                        WHERE created_at < ? AND status = 'MONITORING'
                    ''', (cutoff_time,))
                    
                    conn.commit()
                    print(f"🧹 Cleaned up {count} expired signals (older than {hours}h)")
                
                return count
                
        except Exception as e:
            print(f"❌ Failed to cleanup expired signals: {e}")
            return 0
