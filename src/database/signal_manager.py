"""
Signal Manager for Two-Stage Crypto Scanner
Handles database operations for pending signals
"""

import sqlite3
import logging
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class SignalManager:
    def __init__(self):
        self.config = self.load_config()
        self.db_path = self.config['database']['path']
        self.ensure_db_directory()
    
    def load_config(self):
        """Load configuration from YAML file"""
        config_path = Path("config/enhanced_config.yaml")
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def ensure_db_directory(self):
        """Ensure database directory exists"""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """Create database tables if they don't exist"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_symbol TEXT NOT NULL,
            signal_direction TEXT NOT NULL,
            signal_timestamp DATETIME NOT NULL,
            price_at_signal REAL NOT NULL,
            cipherb_signal_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(coin_symbol, status)
        );
        
        CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
        CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(coin_symbol);
        CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(signal_timestamp);
        """
        
        try:
            with self.get_connection() as conn:
                conn.executescript(create_sql)
                conn.commit()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def add_signal(self, coin_symbol: str, signal_direction: str, 
                   price_at_signal: float, cipherb_value: float) -> bool:
        """Add a new signal, replacing any existing pending signal for the same coin"""
        
        insert_sql = """
        INSERT OR REPLACE INTO signals 
        (coin_symbol, signal_direction, signal_timestamp, price_at_signal, 
         cipherb_signal_type, status, updated_at)
        VALUES (?, ?, ?, ?, ?, 'PENDING', CURRENT_TIMESTAMP)
        """
        
        try:
            with self.get_connection() as conn:
                conn.execute(insert_sql, (
                    coin_symbol.upper(),
                    signal_direction.upper(),
                    datetime.utcnow().isoformat(),
                    price_at_signal,
                    signal_direction.upper()
                ))
                conn.commit()
                
            logger.info(f"Added {signal_direction} signal for {coin_symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding signal: {e}")
            return False
    
    def get_pending_signals(self) -> List[Dict]:
        """Get all pending signals"""
        select_sql = """
        SELECT * FROM signals 
        WHERE status = 'PENDING'
        ORDER BY signal_timestamp DESC
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(select_sql)
                rows = cursor.fetchall()
                
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting pending signals: {e}")
            return []
    
    def confirm_signal(self, coin_symbol: str) -> bool:
        """Mark a signal as confirmed and delete it"""
        delete_sql = """
        DELETE FROM signals 
        WHERE coin_symbol = ? AND status = 'PENDING'
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(delete_sql, (coin_symbol.upper(),))
                conn.commit()
                
            if cursor.rowcount > 0:
                logger.info(f"Confirmed and deleted signal for {coin_symbol}")
                return True
            else:
                logger.warning(f"No pending signal found for {coin_symbol}")
                return False
                
        except Exception as e:
            logger.error(f"Error confirming signal: {e}")
            return False
    
    def delete_signal(self, coin_symbol: str) -> bool:
        """Delete a specific signal"""
        delete_sql = """
        DELETE FROM signals 
        WHERE coin_symbol = ? AND status = 'PENDING'
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(delete_sql, (coin_symbol.upper(),))
                conn.commit()
                
            if cursor.rowcount > 0:
                logger.info(f"Deleted signal for {coin_symbol}")
                return True
            else:
                logger.warning(f"No signal found for {coin_symbol}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting signal: {e}")
            return False
    
    def cleanup_expired_signals(self):
        """Remove signals older than configured expiry time"""
        expiry_hours = self.config['signals']['expiry_hours']
        cutoff_time = datetime.utcnow() - timedelta(hours=expiry_hours)
        
        delete_sql = """
        DELETE FROM signals 
        WHERE signal_timestamp < ? AND status = 'PENDING'
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(delete_sql, (cutoff_time.isoformat(),))
                conn.commit()
                
            if cursor.rowcount > 0:
                logger.info(f"Cleaned up {cursor.rowcount} expired signals")
                
        except Exception as e:
            logger.error(f"Error cleaning up expired signals: {e}")
    
    def get_signal_count(self) -> int:
        """Get count of pending signals"""
        count_sql = "SELECT COUNT(*) FROM signals WHERE status = 'PENDING'"
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(count_sql)
                return cursor.fetchone()[0]
                
        except Exception as e:
            logger.error(f"Error getting signal count: {e}")
            return 0
    
    def get_signals_by_direction(self, direction: str) -> List[Dict]:
        """Get pending signals filtered by direction"""
        select_sql = """
        SELECT * FROM signals 
        WHERE status = 'PENDING' AND signal_direction = ?
        ORDER BY signal_timestamp DESC
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(select_sql, (direction.upper(),))
                rows = cursor.fetchall()
                
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting signals by direction: {e}")
            return []

def initialize_database():
    """Initialize database and create tables"""
    try:
        signal_manager = SignalManager()
        signal_manager.create_tables()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False

if __name__ == "__main__":
    initialize_database()
