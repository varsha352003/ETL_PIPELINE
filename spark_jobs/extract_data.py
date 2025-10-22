import yfinance as yf
import pandas as pd
from datetime import datetime

def fetch_data(symbol="AAPL",period="1mo",inetrval="1h"):
    
    print(f"Fetching data for {symbol}...")
    df=yf.download(symbol, period=period, interval=inetrval)

    if df.empty:
        print("No data fetched. Check symbol or API limits.")
        return None
    
    df.reset_index(inplace=True)
    df["symbol"]=symbol

    return df

def save_to_csv(df,symbol):
    filename=f"{symbol}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename,index=False)
    print(f"Data saved to {filename}")

if __name__=="__main__":
    symbol="AAPL"
    df=fetch_data(symbol=symbol,period="1mo",inetrval="1h")
    
    if df is not None:
        save_to_csv(df,symbol)