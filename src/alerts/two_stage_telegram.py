"""
Two-Stage Telegram Alert System
Handles Stage 1 (monitoring) and Stage 2 (execution) alerts
"""

import asyncio
import logging
import os
import yaml
from pathlib import Path
from typing import List, Dict
from telegram import Bot
from telegram.error import TelegramError
from utils.tradingview_links import get_chart_links

logger = logging.getLogger(__name__)

class TelegramAlerter:
    def __init__(self):
        self.config = self.load_config()
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.stage1_channel = os.getenv('STAGE1_TELEGRAM_CHAT_ID')
        self.stage2_channel = os.getenv('STAGE2_TELEGRAM_CHAT_ID')
        
        if not all([self.bot_token, self.stage1_channel, self.stage2_channel]):
            raise ValueError("Missing required Telegram environment variables")
            
        self.bot = Bot(token=self.bot_token)
    
    def load_config(self):
        """Load configuration from YAML file"""
        config_path = Path("config/enhanced_config.yaml")
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def format_market_cap(self, market_cap):
        """Format market cap in millions"""
        try:
            if market_cap is None or market_cap == 0:
                return "N/A"
            return f"${market_cap / 1_000_000:.0f}M"
        except:
            return "N/A"
    
    def format_price_change(self, price_change):
        """Format price change percentage"""
        try:
            if price_change is None:
                return "N/A"
            sign = "+" if price_change > 0 else ""
            return f"{sign}{price_change:.2f}%"
        except:
            return "N/A"
    
    def format_price(self, price):
        """Format price with appropriate decimal places"""
        try:
            if price is None or price == 0:
                return "N/A"
            
            if price >= 1:
                return f"${price:.4f}"
            else:
                return f"${price:.6f}"
        except:
            return "N/A"
    
    def create_stage1_message(self, buy_signals: List[Dict], sell_signals: List[Dict]) -> str:
        """
        Create Stage 1 monitoring alert message
        
        Args:
            buy_signals: List of BUY signal data
            sell_signals: List of SELL signal data
            
        Returns:
            str: Formatted message for Stage 1
        """
        try:
            message_parts = ["🔍 **STAGE 1: CipherB Signals Detected**\n"]
            
            # Add BUY signals
            if buy_signals:
                message_parts.append(f"📊 **BUY Signals ({len(buy_signals)}):**")
                for signal in buy_signals[:10]:  # Limit to 10 to avoid message length issues
                    symbol = signal.get('symbol', 'UNKNOWN')
                    price = self.format_price(signal.get('current_price'))
                    market_cap = self.format_market_cap(signal.get('market_cap'))
                    price_change = self.format_price_change(signal.get('price_change_percentage_24h'))
                    
                    # Get chart link
                    links = get_chart_links(symbol)
                    chart_link = links['ha_15m']
                    
                    message_parts.append(
                        f"• {symbol} - {price} | MC: {market_cap} | {price_change} | [📈 Chart]({chart_link})"
                    )
                
                if len(buy_signals) > 10:
                    message_parts.append(f"... and {len(buy_signals) - 10} more")
                
                message_parts.append("")  # Empty line
            
            # Add SELL signals
            if sell_signals:
                message_parts.append(f"📊 **SELL Signals ({len(sell_signals)}):**")
                for signal in sell_signals[:10]:  # Limit to 10
                    symbol = signal.get('symbol', 'UNKNOWN')
                    price = self.format_price(signal.get('current_price'))
                    market_cap = self.format_market_cap(signal.get('market_cap'))
                    price_change = self.format_price_change(signal.get('price_change_percentage_24h'))
                    
                    # Get chart link
                    links = get_chart_links(symbol)
                    chart_link = links['ha_15m']
                    
                    message_parts.append(
                        f"• {symbol} - {price} | MC: {market_cap} | {price_change} | [📉 Chart]({chart_link})"
                    )
                
                if len(sell_signals) > 10:
                    message_parts.append(f"... and {len(sell_signals) - 10} more")
            
            if not buy_signals and not sell_signals:
                message_parts.append("No new signals detected in this cycle.")
            
            return "\n".join(message_parts)
            
        except Exception as e:
            logger.error(f"Error creating Stage 1 message: {e}")
            return "Error creating Stage 1 message"
    
    def create_stage2_message(self, buy_confirmations: List[Dict], sell_confirmations: List[Dict]) -> str:
        """
        Create Stage 2 execution alert message
        
        Args:
            buy_confirmations: List of BUY confirmation data
            sell_confirmations: List of SELL confirmation data
            
        Returns:
            str: Formatted message for Stage 2
        """
        try:
            message_parts = ["✅ **STAGE 2: Entry Confirmations**\n"]
            
            # Add BUY confirmations
            if buy_confirmations:
                message_parts.append(f"🟢 **BUY Confirmations ({len(buy_confirmations)}):**")
                for conf in buy_confirmations[:10]:  # Limit to 10
                    symbol = conf.get('symbol', 'UNKNOWN')
                    price = self.format_price(conf.get('current_price'))
                    market_cap = self.format_market_cap(conf.get('market_cap'))
                    
                    # Get both chart links
                    links = get_chart_links(symbol)
                    ha_link = links['ha_15m']
                    std_link = links['standard_5m']
                    
                    message_parts.append(
                        f"• {symbol} - {price} | MC: {market_cap} | 9EMA crossed above 18EMA"
                    )
                    message_parts.append(
                        f"  15m: [📈 HA Chart]({ha_link}) | 5m: [📈 Entry Chart]({std_link})"
                    )
                
                if len(buy_confirmations) > 10:
                    message_parts.append(f"... and {len(buy_confirmations) - 10} more")
                
                message_parts.append("")  # Empty line
            
            # Add SELL confirmations
            if sell_confirmations:
                message_parts.append(f"🔴 **SELL Confirmations ({len(sell_confirmations)}):**")
                for conf in sell_confirmations[:10]:  # Limit to 10
                    symbol = conf.get('symbol', 'UNKNOWN')
                    price = self.format_price(conf.get('current_price'))
                    market_cap = self.format_market_cap(conf.get('market_cap'))
                    
                    # Get both chart links
                    links = get_chart_links(symbol)
                    ha_link = links['ha_15m']
                    std_link = links['standard_5m']
                    
                    message_parts.append(
                        f"• {symbol} - {price} | MC: {market_cap} | 9EMA crossed below 18EMA"
                    )
                    message_parts.append(
                        f"  15m: [📉 HA Chart]({ha_link}) | 5m: [📉 Entry Chart]({std_link})"
                    )
                
                if len(sell_confirmations) > 10:
                    message_parts.append(f"... and {len(sell_confirmations) - 10} more")
            
            if not buy_confirmations and not sell_confirmations:
                message_parts.append("No new confirmations in this cycle.")
            
            return "\n".join(message_parts)
            
        except Exception as e:
            logger.error(f"Error creating Stage 2 message: {e}")
            return "Error creating Stage 2 message"
    
    async def send_stage1_alert(self, buy_signals: List[Dict], sell_signals: List[Dict]) -> bool:
        """
        Send Stage 1 monitoring alert
        
        Args:
            buy_signals: List of BUY signals
            sell_signals: List of SELL signals
            
        Returns:
            bool: True if successful
        """
        try:
            message = self.create_stage1_message(buy_signals, sell_signals)
            
            await self.bot.send_message(
                chat_id=self.stage1_channel,
                text=message,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            
            logger.info(f"Stage 1 alert sent: {len(buy_signals)} BUY, {len(sell_signals)} SELL")
            return True
            
        except TelegramError as e:
            logger.error(f"Telegram error sending Stage 1 alert: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending Stage 1 alert: {e}")
            return False
    
    async def send_stage2_alert(self, buy_confirmations: List[Dict], sell_confirmations: List[Dict]) -> bool:
        """
        Send Stage 2 execution alert
        
        Args:
            buy_confirmations: List of BUY confirmations
            sell_confirmations: List of SELL confirmations
            
        Returns:
            bool: True if successful
        """
        try:
            message = self.create_stage2_message(buy_confirmations, sell_confirmations)
            
            await self.bot.send_message(
                chat_id=self.stage2_channel,
                text=message,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            
            logger.info(f"Stage 2 alert sent: {len(buy_confirmations)} BUY, {len(sell_confirmations)} SELL")
            return True
            
        except TelegramError as e:
            logger.error(f"Telegram error sending Stage 2 alert: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending Stage 2 alert: {e}")
            return False
    
    def send_stage1_alert_sync(self, buy_signals: List[Dict], sell_signals: List[Dict]) -> bool:
        """Synchronous wrapper for Stage 1 alerts"""
        try:
            return asyncio.run(self.send_stage1_alert(buy_signals, sell_signals))
        except Exception as e:
            logger.error(f"Error in sync Stage 1 alert: {e}")
            return False
    
    def send_stage2_alert_sync(self, buy_confirmations: List[Dict], sell_confirmations: List[Dict]) -> bool:
        """Synchronous wrapper for Stage 2 alerts"""
        try:
            return asyncio.run(self.send_stage2_alert(buy_confirmations, sell_confirmations))
        except Exception as e:
            logger.error(f"Error in sync Stage 2 alert: {e}")
            return False

# Global instance for easy import
# telegram_alerter = TelegramAlerter()

_telegram_alerter = None

def get_telegram_alerter():
    """Get or create telegram alerter instance"""
    global _telegram_alerter
    if _telegram_alerter is None:
        _telegram_alerter = TelegramAlerter()
    return _telegram_alerter

def send_monitoring_alert(buy_signals, sell_signals):
    """Convenience function for Stage 1 alerts"""
    return get_telegram_alerter().send_stage1_alert_sync(buy_signals, sell_signals)

def send_execution_alert(buy_confirmations, sell_confirmations):
    """Convenience function for Stage 2 alerts"""
    return get_telegram_alerter().send_stage2_alert_sync(buy_confirmations, sell_confirmations)

if __name__ == "__main__":
    # Test with sample data
    sample_buy = [
        {
            'symbol': 'BTCUSDT',
            'current_price': 67432.0,
            'market_cap': 1340000000,
            'price_change_percentage_24h': 2.34
        }
    ]
    
    sample_sell = [
        {
            'symbol': 'ETHUSDT',
            'current_price': 2845.0,
            'market_cap': 342000000,
            'price_change_percentage_24h': -1.12
        }
    ]
    
    alerter = TelegramAlerter()
    print("Stage 1 message:")
    print(alerter.create_stage1_message(sample_buy, sample_sell))
    print("\nStage 2 message:")
    print(alerter.create_stage2_message(sample_buy, []))
