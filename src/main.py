#!/usr/bin/env python3
"""
Main entry point for the Two-Stage High-Risk Crypto Scanner
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from enhanced_data_fetcher import fetch_market_data
from enhanced_analyzer_15m import analyze_15m
from enhanced_analyzer_5m import analyze_5m
from database.signal_manager import initialize_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_scanner.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def validate_environment():
    """Validate required environment variables"""
    required_vars = [
        'TELEGRAM_BOT_TOKEN',
        'STAGE1_TELEGRAM_CHAT_ID',
        'STAGE2_TELEGRAM_CHAT_ID'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return False
    
    return True

def main():
    """Main function to handle command line arguments and route execution"""
    parser = argparse.ArgumentParser(description='Two-Stage High-Risk Crypto Scanner')
    parser.add_argument('--fetch-data', action='store_true', 
                       help='Fetch and cache market data from CoinGecko')
    parser.add_argument('--analyze-15m', action='store_true',
                       help='Run 15-minute CipherB analysis')
    parser.add_argument('--analyze-5m', action='store_true',
                       help='Run 5-minute EMA confirmation analysis')
    parser.add_argument('--init-db', action='store_true',
                       help='Initialize database tables')
    
    args = parser.parse_args()
    
    try:
        # Initialize database if requested
        if args.init_db:
            logger.info("Initializing database...")
            initialize_database()
            logger.info("Database initialized successfully")
            return
        
        # Validate environment for other operations
        if not validate_environment():
            return
        
        # Fetch market data if requested
        if args.fetch_data:
            logger.info("Starting market data fetch...")
            fetch_market_data()
            logger.info("Market data fetch completed")
        
        # Run 15-minute analysis if requested
        if args.analyze_15m:
            logger.info("Starting 15-minute CipherB analysis...")
            analyze_15m()
            logger.info("15-minute analysis completed")
        
        # Run 5-minute analysis if requested
        if args.analyze_5m:
            logger.info("Starting 5-minute EMA confirmation analysis...")
            analyze_5m()
            logger.info("5-minute analysis completed")
        
        # If no arguments provided, show help
        if not any(vars(args).values()):
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
