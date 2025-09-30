import requests
import json
import os
from datetime import datetime, timedelta, time
import pytz
from collections import defaultdict

# ====== CONFIG ======
# Get from GitHub Secrets
API_KEY = os.environ.get("OANDA_API_KEY")
ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID")
BASE_URL = "https://api-fxpractice.oanda.com/v3"

# Telegram config
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

INSTRUMENTS = {
    "XAUUSD": "XAU_USD",
    "NAS100": "NAS100_USD",
    "US30": "US30_USD"
}

# Fixed Configuration
SELECTED_INSTRUMENTS = ["XAUUSD", "NAS100", "US30"]
BUCKET_MINUTES = 60  # 1 hour bucket
ENABLE_TELEGRAM_ALERTS = True
SKIP_WEEKENDS = True
THRESHOLD_MULTIPLIER = 0.1
GRANULARITY = "M15"  # 15 minutes

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc
headers = {"Authorization": f"Bearer {API_KEY}"}

# Number of trading days to use for averaging
TRADING_DAYS_FOR_AVERAGE = 21

# ====== ALERT MEMORY (GitHub Actions) ======
def load_alerted_candles():
    """For GitHub Actions, we can't persist files between runs"""
    return set()

def save_alerted_candles(alerted_set):
    """For GitHub Actions, we skip saving"""
    pass

# ====== TELEGRAM ALERT ======
def send_telegram_alert(message):
    if not ENABLE_TELEGRAM_ALERTS:
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials missing!")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        resp = requests.post(url, data=payload, timeout=10)
        if resp.status_code != 200:
            print(f"❌ Telegram alert failed: {resp.text}")
        else:
            print("✅ Telegram alert sent successfully!")
    except Exception as e:
        print(f"❌ Telegram alert exception: {e}")

# ====== OANDA DATA FETCH ======
def fetch_candles(instrument_code, from_time, to_time):
    now_utc = datetime.now(UTC)
    from_time = min(from_time, now_utc)
    to_time = min(to_time, now_utc)
    
    params = {
        "granularity": GRANULARITY,
        "price": "M",
        "from": from_time.isoformat(),
        "to": to_time.isoformat()
    }
    url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/instruments/{instrument_code}/candles"
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
    except Exception as e:
        print(f"❌ Network error for {instrument_code}: {e}")
        return []
    
    if resp.status_code != 200:
        print(f"❌ Failed to fetch {instrument_code} data: {resp.text}")
        return []
    return resp.json().get("candles", [])

# ====== UTILITIES ======
def get_time_bucket(dt_ist):
    """Calculate 1-hour time bucket"""
    bucket_start_minute = (dt_ist.minute // BUCKET_MINUTES) * BUCKET_MINUTES
    bucket_start = dt_ist.replace(minute=bucket_start_minute, second=0, microsecond=0)
    bucket_end = bucket_start + timedelta(minutes=BUCKET_MINUTES)
    return f"{bucket_start.strftime('%I:%M %p')}–{bucket_end.strftime('%I:%M %p')}"

def is_weekend(date):
    """Check if a date is Saturday (5) or Sunday (6)"""
    return date.weekday() in [5, 6]

def get_sentiment(candle):
    o = float(candle["mid"]["o"])
    c = float(candle["mid"]["c"])
    return "🟩" if c > o else "🟥" if c < o else "▪️"

def compute_bucket_averages(code):
    """Compute average volumes for each time bucket"""
    bucket_volumes = defaultdict(list)
    today_ist = datetime.now(IST).date()
    now_utc = datetime.now(UTC)
    
    trading_days_collected = 0
    days_back = 1
    max_lookback = 60
    
    while trading_days_collected < TRADING_DAYS_FOR_AVERAGE and days_back < max_lookback:
        day_ist = today_ist - timedelta(days=days_back)
        
        if SKIP_WEEKENDS and is_weekend(day_ist):
            days_back += 1
            continue
            
        start_ist = IST.localize(datetime.combine(day_ist, time(0, 0)))
        end_ist = IST.localize(datetime.combine(day_ist + timedelta(days=1), time(0, 0)))
        
        start_utc = start_ist.astimezone(UTC)
        end_utc = min(end_ist.astimezone(UTC), now_utc)
        
        candles = fetch_candles(code, start_utc, end_utc)
        
        if candles:
            trading_days_collected += 1
            
            for c in candles:
                if not c.get("complete", True):
                    continue
                try:
                    t_utc = datetime.strptime(c["time"], "%Y-%m-%dT%H:%M:%S.%f000Z")
                except ValueError:
                    t_utc = datetime.strptime(c["time"], "%Y-%m-%dT%H:%M:%S.000Z")
                t_ist = t_utc.replace(tzinfo=UTC).astimezone(IST)
                bucket = get_time_bucket(t_ist)
                bucket_volumes[bucket].append(c["volume"])
        
        days_back += 1
    
    return {b: (sum(vs) / len(vs)) for b, vs in bucket_volumes.items() if vs}

# ====== CORE PROCESS ======
def check_recent_spikes(name, code):
    """Check only the most recent candles for spikes"""
    bucket_avg = compute_bucket_averages(code)
    now_utc = datetime.now(UTC)
    
    # Get last 3 candles only (45 minutes)
    from_time = now_utc - timedelta(minutes=45)
    
    candles = fetch_candles(code, from_time, now_utc)
    if not candles:
        return []
    
    spike_alerts = []
    
    for c in candles:
        # Skip incomplete candles
        if not c.get("complete", True):
            continue
            
        try:
            t_utc = datetime.strptime(c["time"], "%Y-%m-%dT%H:%M:%S.%f000Z")
        except ValueError:
            t_utc = datetime.strptime(c["time"], "%Y-%m-%dT%H:%M:%S.000Z")
        t_ist = t_utc.replace(tzinfo=UTC).astimezone(IST)
        
        bucket = get_time_bucket(t_ist)
        
        vol = c["volume"]
        avg = bucket_avg.get(bucket, 0)
        threshold = avg * THRESHOLD_MULTIPLIER if avg else 0
        
        if threshold > 0 and vol > threshold:
            spike_diff = vol - int(threshold)
            sentiment = get_sentiment(c)
            mult = vol / threshold
            
            spike_alerts.append({
                "instrument": name,
                "time": t_ist.strftime('%I:%M %p'),
                "date": t_ist.strftime('%Y-%m-%d'),
                "volume": vol,
                "spike_diff": spike_diff,
                "multiplier": mult,
                "sentiment": sentiment,
                "bucket": bucket
            })
    
    return spike_alerts

# ====== MAIN EXECUTION ======
def run_volume_check():
    all_spikes = []
    
    print(f"\n[{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}] Checking volume spikes...")
    
    for name in SELECTED_INSTRUMENTS:
        code = INSTRUMENTS[name]
        print(f"  Checking {name}...")
        
        spikes = check_recent_spikes(name, code)
        if spikes:
            all_spikes.extend(spikes)
            for spike in spikes:
                print(f"    ⚡ SPIKE: {spike['time']} - Volume {spike['volume']:,} (×{spike['multiplier']:.2f})")
    
    # Send consolidated alert
    if all_spikes:
        print(f"\n⚡ Total spikes found: {len(all_spikes)}")
        
        # Group by instrument
        alert_messages = []
        for spike in all_spikes:
            msg = (
                f"🔍 *{spike['instrument']}*\n"
                f"🕒 Time: {spike['time']} IST\n"
                f"📅 Date: {spike['date']}\n"
                f"📊 Volume: {spike['volume']:,} (+{spike['spike_diff']:,})\n"
                f"📈 Multiplier: ×{spike['multiplier']:.2f}\n"
                f"💹 Sentiment: {spike['sentiment']}\n"
                f"⏰ Bucket: {spike['bucket']}"
            )
            alert_messages.append(msg)
        
        full_alert = "⚡ *VOLUME SPIKE ALERT* ⚡\n\n" + "\n\n➖➖➖➖➖➖➖➖➖\n\n".join(alert_messages)
        send_telegram_alert(full_alert)
    else:
        print("  ✓ No volume spikes detected")

if __name__ == "__main__":
    run_volume_check()
