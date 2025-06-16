import requests
import pandas as pd
import time
import os
import csv
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN1")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID1")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

session = requests.Session()
TOKEN_CSV_PATH = "token_listesi.csv"
PUMP_SCORE_THRESHOLD = 6
QUOTE = "USDT"
ROLLBACK_THRESHOLD = 3  # % olarak, pump yaptıktan sonra %3 ve üstü geri çekilme varsa atla

LEVERAGED_SUFFIXES = [
    "3S", "3L", "5S", "5L", "2S", "2L", "4S", "4L",
    "BULL", "BEAR", "DOWN", "UP", "HALF", "HEDGE"
]

def is_spot(pair, symbol):
    pair = pair.upper()
    symbol = symbol.upper()
    for suf in LEVERAGED_SUFFIXES:
        if pair.endswith(suf) or symbol.endswith(suf):
            return False
    return True

def get_erc20_transfers(token_address, start_timestamp, end_timestamp=None):
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": token_address,
        "starttimestamp": start_timestamp,
        "endtimestamp": end_timestamp if end_timestamp else "",
        "sort": "desc",
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        r = session.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") != "1":
            return []
        return data["result"]
    except Exception as e:
        print("Etherscan error:", e)
        return []

def get_coin_data(coin_id):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
        r = session.get(url, timeout=10)
        data = r.json()
        if "market_data" not in data:
            print(f"CoinGecko error ({coin_id}): 'market_data' eksik! Veri: {data}")
            return None, None
        price = data["market_data"]["current_price"]["usd"]
        supply = data["market_data"]["circulating_supply"]
        return price, supply
    except Exception as e:
        print(f"CoinGecko error ({coin_id}): {e}")
        return None, None

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        session.post(url, data=payload, timeout=15)
    except Exception as e:
        print(f"Telegram mesajı gönderilemedi: {e}")

def whale_summary(transfers, price, supply, period_name=""):
    incoming = sum(float(tx["value"]) / (10 ** int(tx["tokenDecimal"])) for tx in transfers if tx['to'] != '0x0000000000000000000000000000000000000000')
    outgoing = sum(float(tx["value"]) / (10 ** int(tx["tokenDecimal"])) for tx in transfers if tx['from'] != '0x0000000000000000000000000000000000000000')
    net = incoming - outgoing
    usd_in = incoming * price if price else 0
    usd_out = outgoing * price if price else 0
    usd_net = net * price if price else 0
    perc_in = (incoming / supply) * 100 if supply else 0
    perc_out = (outgoing / supply) * 100 if supply else 0
    perc_net = (net / supply) * 100 if supply else 0
    high_in = perc_in > 0.5
    high_out = perc_out > 0.5

    whale_lines = [f"⏱️ <b>{period_name} Balina Hareketi</b>"]
    if incoming > 0:
        whale_lines.append(f"🟩 Giriş: <b>{incoming:,.0f}</b> ({perc_in:.4f}%) ≈ ${usd_in:,.0f}")
        if high_in:
            whale_lines.append("🚨 YÜKSEK BALİNA GİRİŞİ!")
    if outgoing > 0:
        whale_lines.append(f"🟥 Çıkış: <b>{outgoing:,.0f}</b> ({perc_out:.4f}%) ≈ ${usd_out:,.0f}")
        if high_out:
            whale_lines.append("⚠️ YÜKSEK BALİNA ÇIKIŞI!")
    whale_lines.append(f"🟦 Net: <b>{net:,.0f}</b> ({perc_net:.4f}%) ≈ ${usd_net:,.0f}")
    return "\n".join(whale_lines)

def get_binance_pairs(quote=QUOTE):
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = session.get(url, timeout=15).json()
    pairs = {s['symbol'].upper() for s in data['symbols'] if s['quoteAsset'] == quote and s['status'] == 'TRADING'}
    print(f"[LOG] Binance Tradable Pairs found: {len(pairs)}")
    return pairs

def get_gateio_pairs(quote=QUOTE):
    url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    data = session.get(url, timeout=15).json()
    pairs = {d['id'].replace('_', '').upper() for d in data if d['quote'].upper() == quote and d['trade_status'] == 'tradable'}
    print(f"[LOG] Gateio Tradable Pairs found: {len(pairs)}")
    return pairs

def get_binance_ohlc(symbol, interval="1m", limit=50):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    try:
        r = session.get(url, params=params, timeout=10)
        data = r.json()
        df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close",
                                         "volume", "close_time", "qav", "num_trades",
                                         "taker_base_vol", "taker_quote_vol", "ignore"])
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        return df
    except Exception as e:
        print(f"[LOG] Binance OHLC error for {symbol}: {e}")
        return None

def get_gateio_ohlc(symbol, interval="1m", limit=50):
    url = f"https://api.gateio.ws/api/v4/spot/candlesticks"
    params = {"currency_pair": symbol.upper(), "interval": interval, "limit": limit}
    try:
        r = session.get(url, params=params, timeout=10)
        data = r.json()
        df = pd.DataFrame(data, columns=[
            "close_time", "open", "close", "high", "low", "base_volume", "quote_volume", "trade_count"
        ])
        df = df.iloc[::-1].reset_index(drop=True)
        df[["open", "high", "low", "close", "base_volume"]] = df[["open", "high", "low", "close", "base_volume"]].astype(float)
        df = df.rename(columns={"base_volume": "volume"})
        return df
    except Exception as e:
        print(f"[LOG] Gateio OHLC error for {symbol}: {e}")
        return None

def price_change(df, period):
    if len(df) < period:
        return 0
    price_now = df['close'].iloc[-1]
    price_past = df['close'].iloc[-period]
    return (price_now - price_past) / price_past * 100 if price_past != 0 else 0

def pump_score(df):
    try:
        window = 10
        last = 3
        avg_vol = df['volume'].iloc[-window-last:-last].mean()
        recent_vol = df['volume'].iloc[-last:].mean()
        vol_ratio = recent_vol / (avg_vol + 1e-9)
        price_now = df['close'].iloc[-1]
        price_breakout = df['high'].iloc[-window-last:-last].max()
        price_chg_win = (price_now - df['close'].iloc[-window-last]) / (df['close'].iloc[-window-last] + 1e-9) * 100

        vol_now = df['volume'].iloc[-5:].sum()
        vol_prev = df['volume'].iloc[-10:-5].sum()
        vol_chg = (vol_now - vol_prev) / (vol_prev + 1e-8) * 100
        vol_chg = min(vol_chg, 100_000)
        price_prev6 = df['close'].iloc[-6]
        price_chg6 = (price_now - price_prev6) / (price_prev6 + 1e-9) * 100
        delta = df['close'].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        roll_up = up.rolling(14).mean()
        roll_down = down.rolling(14).mean()
        rs = roll_up / (roll_down + 1e-8)
        rsi = 100.0 - (100.0 / (1.0 + rs)).iloc[-1]
        obv = [0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i-1]:
                obv.append(obv[-1] + df['volume'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                obv.append(obv[-1] - df['volume'].iloc[i])
            else:
                obv.append(obv[-1])
        obv_up = obv[-1] > obv[-6]
        highest = df['high'].rolling(5).max()
        lowest = df['low'].rolling(5).min()
        willr = -100 * (highest - df['close']) / (highest - lowest + 1e-8)
        willr_val = willr.iloc[-1]
        breakout = df['close'].iloc[-1] > df['high'].iloc[-15:-1].max() * 1.01
        ema25 = df['close'].rolling(25).mean()
        ema_kirma = price_now > ema25.iloc[-1]
        son2hacim_up = df['volume'].iloc[-1] > df['volume'].iloc[-2] > df['volume'].iloc[-3]

        pump_start = (
            vol_ratio > 3.0 and
            price_now > price_breakout * 1.01 and
            price_chg_win > 15
        )

        score = 0
        if pump_start:
            score += 5
        if vol_chg > 500: score += 3
        if 5 < price_chg6 < 20: score += 2
        if rsi > 60 and obv_up: score += 1
        if willr_val > -30: score += 1
        if breakout: score += 1
        if ema_kirma: score += 1
        if son2hacim_up: score += 1

        if rsi > 80 and price_chg6 < 3:
            score -= 2

        return score, {
            "vol_chg": vol_chg,
            "price_chg": price_chg6,
            "rsi": rsi,
            "obv_up": obv_up,
            "willr": willr_val,
            "breakout": breakout,
            "price": price_now,
            "vol_ratio": vol_ratio,
            "price_chg_win": price_chg_win
        }
    except Exception as e:
        print(f"[LOG] Pump score error: {e}")
        return 0, {}

def coin_has_rollback(df, rollback_thr=ROLLBACK_THRESHOLD, period=60):
    if len(df) < period:
        return False
    window = df['close'].iloc[-period:]
    peak = window.max()
    now = df['close'].iloc[-1]
    if peak == 0:
        return False
    rollback = (peak - now) / peak * 100
    return rollback > rollback_thr

def stablecoin_whale_report(symbol, cg_id, token_addr):
    price, supply = get_coin_data(cg_id)
    if not (price and supply):
        return None
    now = int(time.time())
    periods = [
        ("15dk", now - 15*60, now),
        ("30dk", now - 30*60, now),
        ("1s",   now - 60*60, now),
        ("4s",   now - 4*60*60, now),
        ("24s",  now - 24*60*60, now)
    ]
    report = [f"<b>{symbol} (SABIT COIN)</b>\n💰 Fiyat: <code>{price:,.6f}</code>"]
    for pname, start, end in periods:
        transfers = get_erc20_transfers(token_addr, start, end)
        if transfers:
            report.append(whale_summary(transfers, price, supply, pname))
    return "\n\n".join(report)

def coin_whale_report(symbol, cg_id, token_addr, price, supply):
    now = int(time.time())
    periods = [
        ("15dk", now - 15*60, now),
        ("30dk", now - 30*60, now),
        ("1s",   now - 60*60, now),
        ("4s",   now - 4*60*60, now),
        ("24s",  now - 24*60*60, now)
    ]
    lines = []
    for pname, start, end in periods:
        transfers = get_erc20_transfers(token_addr, start, end)
        if transfers:
            lines.append(whale_summary(transfers, price, supply, pname))
    return "\n\n".join(lines)

def main():
    print(f"{datetime.now().strftime('%H:%M:%S')} ⏳ Tarama başladı...")

    binance_pairs = get_binance_pairs()
    gateio_pairs = get_gateio_pairs()

    toplam_coin = 0
    taranan_coin = 0

    with open(TOKEN_CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            symbol = row['symbol'].upper()
            borsa = row['borsa'].lower()
            pair = row['pair'].replace('_','').upper()  # hem SXPUSDC hem SXP_USDC yakalansın
            token_addr = row.get('token_address')
            cg_id = row.get('coingecko_id')
            is_stable = str(row.get('is_stable', 'False') or 'False').strip().lower() == "true"

            # LOG: Coin bazında işleme başlandı
            print(f"[LOG] {symbol} ({pair}) - {borsa} - Stable: {is_stable}")

            # 1. SABIT COIN ise her zaman balina hareketi ve fiyat raporu gönder
            if is_stable and token_addr and cg_id:
                print(f"[LOG] {symbol} is SABIT COIN, sending whale/price info only.")
                report = stablecoin_whale_report(symbol, cg_id, token_addr)
                if report:
                    send_telegram_message(report)
                    time.sleep(2)
                continue

            # 2. Sadece spot coinleri (ETF, kaldıraçlı hariç) filtrele
            if not is_spot(pair, symbol):
                print(f"[LOG] {symbol} is not spot, skipping.")
                continue

            toplam_coin += 1

            # 3. Teknik analiz (pump_score)
            df = None
            if borsa == 'binance':
                if pair not in binance_pairs:
                    print(f"[LOG] {symbol} ({pair}) not found in Binance tradable pairs, skipping.")
                    continue
                print(f"[LOG] Fetching Binance OHLC for {pair}")
                df = get_binance_ohlc(pair)
            elif borsa == 'gateio':
                if pair not in gateio_pairs:
                    print(f"[LOG] {symbol} ({pair}) not found in Gateio tradable pairs, skipping.")
                    continue
                print(f"[LOG] Fetching Gateio OHLC for {pair}")
                df = get_gateio_ohlc(pair)
            else:
                print(f"[LOG] Unknown borsa: {borsa}, skipping.")
                continue

            if df is None or len(df) < 20:
                print(f"[LOG] OHLC data insufficient for {symbol} ({pair}), skipping.")
                continue

            # 4. Pump yapıp dönmüş olanları atla
            if coin_has_rollback(df, period=60):
                print(f"[LOG] {symbol} ({pair}) has rolled back after pump, skipping.")
                continue

            # 5. Pump adayı tespiti
            score, det = pump_score(df)
            taranan_coin += 1
            print(f"[LOG] {symbol} ({pair}) pump_score: {score}")

            if score < PUMP_SCORE_THRESHOLD:
                print(f"[LOG] {symbol} ({pair}) score < threshold, skipping.")
                continue

            # 6. Pump adayı ise balina hareketi de ekle
            whale_lines = ""
            price, supply = None, None
            if token_addr and cg_id:
                price, supply = get_coin_data(cg_id)
                if price and supply:
                    whale_lines = coin_whale_report(symbol, cg_id, token_addr, price, supply)

            # 7. Telegram mesajı
            sinyal_gucu = "ÇOK GÜÇLÜ" if score >= 9 else "GÜÇLÜ" if score >= 7 else "ORTA"
            hedef_yazi = "%20+ hedef" if score >= 9 else "%10–20 potansiyel" if score >= 7 else "%4–10 potansiyel"
            msg = (
                f"<b>{symbol} ({pair})</b> 🚀 <b>PUMP ADAYI!</b> (Skor: {score})\n"
                f"💹 Borsa: {borsa.upper()}\n"
                f"💰 Fiyat: <code>{det.get('price',0):.6f}</code>\n"
                f"📈 Hacim Artışı: <code>{det.get('vol_chg',0):.2f}%</code>\n"
                f"📉 Fiyat Artışı: <code>{det.get('price_chg',0):.2f}%</code>\n"
                f"🔹 RSI: <code>{det.get('rsi',0):.1f}</code> | OBV: {'YUKARI' if det.get('obv_up') else 'ZAYIF'}\n"
                f"🔸 Williams %R: <code>{det.get('willr',0):.2f}</code> | Breakout: {'VAR' if det.get('breakout') else 'YOK'}\n"
                f"📊 Pump Başlangıcı Sinyali: {'VAR' if score>=PUMP_SCORE_THRESHOLD and det.get('vol_ratio',0)>3 and det.get('price_chg_win',0)>15 else 'YOK'}\n"
                f"🎯 Sinyal Gücü: {sinyal_gucu}\n🎯 Beklenen hedef: {hedef_yazi}\n"
                f"{whale_lines}"
            )
            send_telegram_message(msg)
            time.sleep(2)

    try:
        send_telegram_message(f"🧮 Tarama tamamlandı! Toplam coin: {taranan_coin}/{toplam_coin}")
        print(f"[LOG] 🧮 Tarama tamamlandı! Toplam coin: {taranan_coin}/{toplam_coin}")
    except:
        pass

if __name__ == '__main__':
    main()