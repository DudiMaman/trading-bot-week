# bot/risk.py

from dataclasses import dataclass

@dataclass
class RiskManager:
    equity: float
    risk_per_trade: float = 0.005     # 0.5% כברירת מחדל
    max_position_pct: float = 0.10    # עד 10% הון לפוזיציה

    def update_equity(self, new_equity: float):
        self.equity = float(new_equity)

    def position_size(self, entry: float, sl: float) -> float:
        """חישוב כמות לפי סכום בסיכון לעומת R (entry-sl)."""
        R = abs(entry - sl)
        if R <= 0:
            return 0.0
        cash_risk = self.equity * self.risk_per_trade
        qty_by_risk = cash_risk / R
        qty_by_cap  = (self.equity * self.max_position_pct) / max(entry, 1e-9)
        return max(0.0, min(qty_by_risk, qty_by_cap))


class TradeManager:
    """
    מנהל עסקה שמכיר:
      - atr_k_sl: מרחק SL יחסית ל-ATR
      - r1_R, r2_R: יעדי רווח ביחס ל-R
      - p1_pct, p2_pct: חלק יחסי לסגירה ב-TP1/TP2
      - be_after_R: כמה R צריך לעבור כדי להעביר SL ל-B/E
      - trail_atr_k / atr_trail: טריילינג לפי ATR
      - max_bars_in_trade: יציאה בכוח אחרי מספר נרות
    אם יגיעו פרמטרים לא מוכרים — נתעלם (compat).
    """

    def __init__(self,
                 atr_k_sl: float = 1.5,
                 r1_R: float = 1.0,
                 r2_R: float = 2.0,
                 p1_pct: float = 0.5,
                 p2_pct: float = 0.5,
                 be_after_R: float = 0.8,
                 trail_atr_k: float = 1.2,
                 max_bars_in_trade: int = 48,
                 **kwargs):
        # kwargs נבלע כדי לא להישבר אם יש מפתח עודף בקובץ הקונפיג
        self.atr_k_sl = float(atr_k_sl)
        self.r1_R = float(r1_R)
        self.r2_R = float(r2_R)
        self.p1_pct = float(p1_pct)
        self.p2_pct = float(p2_pct)
        self.be_after_R = float(be_after_R)
        self.trail_atr_k = float(trail_atr_k)
        self.atr_trail = self.trail_atr_k  # אליאס לשם שבו משתמש run_live_week
        self.max_bars_in_trade = int(max_bars_in_trade)

    def trail_level(self, side: str, price: float, atr_now: float, after_tp1: bool) -> float:
        """
        טריילינג SL לפי ATR. אם עברנו TP1, הטריילינג אגרסיבי יותר מעט (אפשר להשאיר אותו זהה).
        """
        k = self.trail_atr_k
        # אפשר להקשיח אחרי TP1: k *= 0.9 או דומה — נשאיר פשוט
        if side == 'long':
            return price - k * atr_now
        else:
            return price + k * atr_now
