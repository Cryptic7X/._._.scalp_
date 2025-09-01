"""
Two-Stage Telegram Alert System
Stage 1: CipherB Signal Detection (Monitoring Alerts)
Stage 2: VWMA Cross Confirmation (Execution Alerts)
"""

import os
import requests
from datetime import datetime, timedelta

def send_stage1_monitoring_alert(signal_data):
    """
    Send Stage 1 alert: CipherB signal detected, monitoring VWMA cross
    Channel: Monitoring alerts channel
    """
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('STAGE1_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️ Stage 1 alert: Missing Telegram credentials")
        return False
    
    try:
        symbol = signal_data['symbol']
        direction = signal_data['direction']
        wt1 = signal_data['wt1']
        wt2 = signal_data['wt2']
        vwma_target = signal_data['vwma_target']
        current_price = signal_data['current_price']
        exchange = signal_data['exchange']
        
        # Format price and VWMA
        if current_price < 0.001:
            price_fmt = f"${current_price:.8f}"
            vwma_fmt = f"${vwma_target:.8f}"
        elif current_price < 0.01:
            price_fmt = f"${current_price:.6f}"
            vwma_fmt = f"${vwma_target:.6f}"
        elif current_price < 1:
            price_fmt = f"${current_price:.4f}"
            vwma_fmt = f"${vwma_target:.4f}"
        else:
            price_fmt = f"${current_price:.3f}"
            vwma_fmt = f"${vwma_target:.3f}"
        
        # Create TradingView link
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
        
        # Direction-specific message
        if direction == 'BUY':
            direction_emoji = "🟢"
            action_text = f"close above {vwma_fmt} (VWMA 21)"
            cross_direction = "upward"
        else:
            direction_emoji = "🔴"
            action_text = f"close below {vwma_fmt} (VWMA 21)"
            cross_direction = "downward"
        
        current_time = datetime.now().strftime('%H:%M:%S IST')
        expires_time = (datetime.now() + timedelta(hours=12)).strftime('%H:%M IST')
        
        message = f"""🔍 *CIPHERB SIGNAL DETECTED*
{direction_emoji} *{symbol}/USDT* | 📊 MONITORING VWMA CROSS

⏳ *Waiting for {cross_direction} cross*
   • Current: {price_fmt}
   • Target: {action_text}

📊 *CipherB Analysis:*
   • WT1: {wt1:.1f}, WT2: {wt2:.1f}
   • Direction: *{direction}*
   • Exchange: {exchange}

[📈 TradingView 15m →]({tv_link})

⏰ Expires: {expires_time} (12 hours)
🕐 {current_time}"""

        # Send alert
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown',
            'disable_web_page_preview': False
        }
        
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        print(f"📨 Stage 1 alert sent: {symbol} {direction} monitoring")
        return True
        
    except requests.RequestException as e:
        print(f"❌ Stage 1 alert failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Stage 1 alert error: {e}")
        return False

def send_stage2_execution_alert(execution_data):
    """
    Send Stage 2 alert: VWMA cross confirmed, execute trade now
    Channel: Execution alerts channel
    """
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('STAGE2_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️ Stage 2 alert: Missing Telegram credentials")
        return False
    
    try:
        symbol = execution_data['symbol']
        direction = execution_data['direction']
        wt1 = execution_data['wt1']
        wt2 = execution_data['wt2']
        entry_price = execution_data['entry_price']
        vwma_value = execution_data['vwma_value']
        exchange = execution_data['exchange']
        signal_age = execution_data['signal_age']
        
        # Format prices
        if entry_price < 0.001:
            entry_fmt = f"${entry_price:.8f}"
            vwma_fmt = f"${vwma_value:.8f}"
        elif entry_price < 0.01:
            entry_fmt = f"${entry_price:.6f}"
            vwma_fmt = f"${vwma_value:.6f}"
        elif entry_price < 1:
            entry_fmt = f"${entry_price:.4f}"
            vwma_fmt = f"${vwma_value:.4f}"
        else:
            entry_fmt = f"${entry_price:.3f}"
            vwma_fmt = f"${vwma_value:.3f}"
        
        # Calculate percentage difference
        price_diff = ((entry_price - vwma_value) / vwma_value) * 100
        
        # Create TradingView link
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
        
        # Direction-specific formatting
        if direction == 'BUY':
            main_emoji = "🚀"
            action_emoji = "⬆️"
            action_text = "BREAKOUT"
            cross_text = f"above VWMA: {vwma_fmt} (+{abs(price_diff):.2f}%)"
        else:
            main_emoji = "🔻"
            action_emoji = "⬇️"
            action_text = "BREAKDOWN"
            cross_text = f"below VWMA: {vwma_fmt} (-{abs(price_diff):.2f}%)"
        
        # Format signal age
        age_hours = signal_age.total_seconds() / 3600
        if age_hours < 1:
            age_text = f"{int(signal_age.total_seconds() / 60)}m"
        else:
            hours = int(age_hours)
            minutes = int((age_hours - hours) * 60)
            age_text = f"{hours}h {minutes}m"
        
        current_time = datetime.now().strftime('%H:%M:%S IST')
        
        message = f"""{main_emoji} *VWMA {action_text} CONFIRMED*
{action_emoji} *{symbol}/USDT* | ⚡ *EXECUTE TRADE NOW*

💰 *Entry Details:*
   • Price: {entry_fmt}
   • Cross: {cross_text}
   • Direction: *{direction}*

📊 *Confirmation:*
   • CipherB: WT1={wt1:.1f}, WT2={wt2:.1f}
   • VWMA 21: {vwma_fmt}
   • Exchange: {exchange}

[📈 TradingView 15m →]({tv_link})

🕐 Signal Age: {age_text} | {current_time}

─────────────────────────────
*🎯 CipherB + VWMA Confirmed Setup*"""

        # Send execution alert
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown',
            'disable_web_page_preview': False
        }
        
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        print(f"🚀 Stage 2 execution alert sent: {symbol} {direction}")
        return True
        
    except requests.RequestException as e:
        print(f"❌ Stage 2 alert failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Stage 2 alert error: {e}")
        return False
