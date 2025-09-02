import ccxt
import pandas as pd
from datetime import datetime
import os

def check_arbitrage():
    """Check for arbitrage opportunities between Binance and KuCoin"""
    exchanges = {
        'binance': ccxt.binance(),
        'kucoin': ccxt.kucoin()
    }
    
    trading_pairs = ['XRP/USDT', 'PEPE/USDT', 'DOGE/USDT']
    trade_amount = 50  # USD
    min_profit = 0.50  # Minimum profit in USD
    opportunities = []
    
    for pair in trading_pairs:
        try:
            # Fetch prices from both exchanges
            binance_ticker = exchanges['binance'].fetch_ticker(pair)
            kucoin_ticker = exchanges['kucoin'].fetch_ticker(pair)
            
            binance_bid = binance_ticker['bid']
            binance_ask = binance_ticker['ask']
            kucoin_bid = kucoin_ticker['bid']
            kucoin_ask = kucoin_ticker['ask']
            
            # Skip if any price is missing or zero
            if None in [binance_bid, binance_ask, kucoin_bid, kucoin_ask] or 0 in [binance_bid, binance_ask, kucoin_bid, kucoin_ask]:
                continue
            
            # Check both arbitrage directions
            # Direction 1: Buy on KuCoin, Sell on Binance
            if binance_bid > kucoin_ask:
                buy_amount = trade_amount / kucoin_ask
                gross_proceeds = buy_amount * binance_bid
                fees = trade_amount * 0.002  # 0.1% on each trade
                profit = gross_proceeds - trade_amount - fees
                
                if profit >= min_profit:
                    opportunities.append({
                        'timestamp': datetime.now().isoformat(),
                        'pair': pair,
                        'direction': 'Buy KuCoin / Sell Binance',
                        'buy_exchange': 'KuCoin',
                        'sell_exchange': 'Binance',
                        'buy_price': kucoin_ask,
                        'sell_price': binance_bid,
                        'trade_amount': trade_amount,
                        'profit': round(profit, 2),
                        'fees': round(fees, 2)
                    })
            
            # Direction 2: Buy on Binance, Sell on KuCoin
            if kucoin_bid > binance_ask:
                buy_amount = trade_amount / binance_ask
                gross_proceeds = buy_amount * kucoin_bid
                fees = trade_amount * 0.002  # 0.1% on each trade
                profit = gross_proceeds - trade_amount - fees
                
                if profit >= min_profit:
                    opportunities.append({
                        'timestamp': datetime.now().isoformat(),
                        'pair': pair,
                        'direction': 'Buy Binance / Sell KuCoin',
                        'buy_exchange': 'Binance',
                        'sell_exchange': 'KuCoin',
                        'buy_price': binance_ask,
                        'sell_price': kucoin_bid,
                        'trade_amount': trade_amount,
                        'profit': round(profit, 2),
                        'fees': round(fees, 2)
                    })
                    
        except Exception as e:
            print(f"Error checking {pair}: {str(e)}")
            continue
    
    return opportunities

def save_results(opportunities):
    """Save opportunities to CSV file"""
    if not opportunities:
        print("No arbitrage opportunities found")
        return False
    
    # Create DataFrame
    df = pd.DataFrame(opportunities)
    
    # Save to CSV (append if file exists)
    file_path = 'arbitrage_results.csv'
    
    if os.path.exists(file_path):
        # Append to existing file
        existing_df = pd.read_csv(file_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_csv(file_path, index=False)
    else:
        # Create new file
        df.to_csv(file_path, index=False)
    
    print(f"Saved {len(opportunities)} opportunities to {file_path}")
    return True

if __name__ == "__main__":
    print(f"Running arbitrage check at {datetime.now().isoformat()}")
    opportunities = check_arbitrage()
    save_results(opportunities)
