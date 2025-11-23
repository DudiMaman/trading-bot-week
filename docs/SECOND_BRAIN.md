# Trading Bot – המוח השני (Second Brain)

גרסת עוגן (גיבוי יציב): **SAFE_V1_PROFIT_2025_11_23**  
Branch עוגן: `safe_v1_profit_2025_11_23`  
Branch עבודה פעיל: `main`  
מטרה: **שיפור רווחיות הבוט לאורך זמן, תחת שליטה מלאה בסיכון.**

---

## 1. מה הבוט הזה מנסה לעשות?

1. לסחור אוטומטית בשוק הקריפטו (Bybit, USDT).
2. לשמור על גישה **Risk First** – קודם מגינים על ההון, אחר כך מחפשים רווח.
3. להיות מספיק מתועד כדי שכל שינוי:
   - יהיה ברור *מה* נעשה,
   - *למה* נעשה,
   - והאם שיפר או הרס את הרווחיות.

---

## 2. תשתיות – איפה מה יושב

- **בורסה:** Bybit
- **חיבור:** `CCXTConnector` עם `exchange_id="bybit"`
- **סוג מסחר בפועל:**  
  - `default_type: "swap"` (פיוצ'רס / unified) בקונקטור  
  - equity נמשך מ־UNIFIED/Spot, ונבחר המקסימום.
- **קובץ ריצה עיקרי:**  
  - `bot/run_live_week.py`
- **שרת ריצה (Deploy):**  
  - Render – שירות worker שמריץ את `bot.run_live_week` (ה-branch הפעיל ברנדר יכול להיות main או branch אחר, לפי מה שהוגדר).
- **קובץ קונפיג:**  
  - `bot/config.yml` (אסטרטגיה, ניהול סיכונים, פורטפוליו, קונקטורים).

---

## 3. לוגים ונתוני ביצועים

### קבצי CSV

- `bot/logs/trades.csv`  
  שדות עיקריים:
  - `time` – זמן ב־UTC
  - `connector` – למשל `bybit_ccxt`
  - `symbol` – לדוגמה `BTC/USDT:USDT`
  - `type` – אחד מ: `ENTER`, `TP1`, `TP2`, `SL`, `TIME`
  - `side` – `long` / `short`
  - `price` – מחיר ביצוע
  - `qty` – כמות
  - `pnl` – רווח/הפסד ב־USDT (ביציאות)
  - `equity` – Equity אחרי האירוע

- `bot/logs/equity_curve.csv`  
  - `time`, `equity` – צילום מצב ההון לאורך זמן.

### בסיס נתונים (אם `DATABASE_URL` מוגדר)

- `equity` – היסטוריית Equity.
- `trades` – כל אירועי הטריידים.
- `live_trades` – פוזיציות פתוחות ונסגרות בזמן אמת.

---

## 4. אסטרטגיית המסחר – DonchianTrendADXRSI (תמצית)

ממומש ב־`bot/strategies.py` כ־`DonchianTrendADXRSI` (שם לדוגמה).

עקרונות:

1. **סיגנלים:**
   - `long_setup = True` → כניסה ל־Long אפשרית.
   - `short_setup = True` → כניסה ל־Short אפשרית.
2. **Fallback Donchian:**
   - אם אין בכלל עמודות סיגנל, או שהכול False:
     - נבנה סיגנל פריצת Donchian בסיסי:
       - לונג – פריצה מעל High אחרון אחרי חלון N.
       - שורט – שבירה מתחת Low אחרון.
3. **טיימפריימים:**
   - Low timeframe (ltf): ברירת מחדל `1m`
   - High timeframe (htf): ברירת מחדל `5m`
4. **ATR:**
   - מחושב עם תקופה 14 (`atr`), משמש ל־SL ו־Trail.

התוצאה: בכל בר לאחר עיבוד, יש שורה אחרונה (`row`) עם:
- `close`, `atr`, `long_setup`, `short_setup`, ועוד פיצ'רים.

---

## 5. ניהול סיכונים (RiskManager + TradeManager)

### מקור ההון (Equity)

בקובץ `run_live_week.py`:

- אם בקובץ הקונפיג:  
  `portfolio.equity0 = "auto"`  
  אז הבוט עושה:

  1. שולף יתרה מ־Bybit UNIFIED (`fetch_balance({"type": "UNIFIED"})`).
  2. שולף יתרה מ־Spot רגיל (`fetch_balance()`).
  3. לוקח `equity = max(equity_unified, equity_spot)`.

- אם ערך מספרי (למשל 100): משתמש בו כ־equity התחלתי.

### פרמטרים חשובים בקונפיג

ב־`config.yml` (תחת `portfolio` ו־`trade_manager`):

- `portfolio.risk_per_trade` – אחוז סיכון לעסקה (למשל `0.03` = 3%).
- `portfolio.max_position_pct` – תקרת חשיפה מתוך ההון (למשל `0.70` = 70%).

בפועל:

1. מחשבים `R` – המרחק בין מחיר כניסה ל־SL.
2. מחשבים `qty_risk = (equity * risk_per_trade) / R`.
3. מגבילים ע"י:
   - תקרת חשיפה כללית: `(equity * max_position_pct) / price`
   - התקציב הפנוי כרגע: `remaining_notional / price`
4. לוקחים את המינימום, ומעגלים לפי:
   - `determine_amount_step(market)` – מבוסס precision / step.
   - בודקים גם `min_qty` ו־`min_cost` (אם הבורסה דורשת מינימום).

---

## 6. פתיחת וסגירת פוזיציות – לוגיקה מרכזית

### כניסה לפוזיציה (ENTER)

נמצא בקטע ה־“Entries” ב־`run_live_week.py`:

- התנאים לפתיחה:
  - אין כבר פוזיציה פתוחה לאותו key (connector,symbol).
  - אין cooldown.
  - יש `long_setup` או `short_setup`.
  - יש ATR חיובי.
  - יש `remaining_notional > 0`.
- לאחר חישוב גודל פוזיציה (`qty`) והגדרת:
  - `sl`, `tp1`, `tp2`, `R`
- מתבצעת הזמנה אמיתית לבייביט:

  ```python
  order_id = place_order(conn, sym, order_side, qty, reduce_only=False)
