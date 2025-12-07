import pandas as pd
import numpy as np


def rsi(series: pd.Series, length: int = 14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / length, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / length, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-12)
    return 100 - (100 / (1 + rs))


def adx(df: pd.DataFrame, length: int = 14):
    high, low, close = df["high"], df["low"], df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move

    tr1 = (high - low).abs()
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = tr1.combine(tr2, max).combine(tr3, max).rolling(length).sum()

    plus_di = 100 * (plus_dm.rolling(length).sum() / (tr + 1e-12))
    minus_di = 100 * (minus_dm.rolling(length).sum() / (tr + 1e-12))
    dx = ((plus_di - minus_di).abs() / ((plus_di + minus_di) + 1e-12)) * 100
    return dx.rolling(length).mean()


class DonchianTrendADXRSI:
    """
    כניסה:
      לונג:  close > DonchianHi  &&  LTF מעל EMA200-HTF  &&  ADX>=th  && RSI<=rsi_max
      שורט:  close < DonchianLo  &&  LTF מתחת EMA200-HTF &&  ADX>=th  && RSI>=100-rsi_min
    """

    def __init__(
        self,
        donchian_len: int = 20,
        rsi_len: int = 14,
        rsi_long_max: float = 70,
        rsi_short_min: float = 30,
        adx_len: int = 14,
        adx_min: float = 18,
    ):
        self.dlen = int(donchian_len)
        self.rsi_len = int(rsi_len)
        self.rsi_long_max = float(rsi_long_max)
        self.rsi_short_min = float(rsi_short_min)
        self.adx_len = int(adx_len)
        self.adx_min = float(adx_min)

    def prepare(self, df_ltf: pd.DataFrame, df_htf: pd.DataFrame):
        df = df_ltf.copy()

        # Donchian
        df["donch_hi"] = df["high"].rolling(self.dlen).max()
        df["donch_lo"] = df["low"].rolling(self.dlen).min()

        # RSI/ADX
        df["rsi"] = rsi(df["close"], self.rsi_len)
        df["adx"] = adx(df, self.adx_len)

        # HTF trend proxy (EMA200)
        htf_ema200 = df_htf["close"].ewm(span=200, adjust=False).mean()
        htf_ema200 = htf_ema200.reindex(df.index, method="ffill")
        df["trend_up"] = (df["close"] > htf_ema200).astype(int)
        df["trend_down"] = (df["close"] < htf_ema200).astype(int)

        # Raw setups
        df["long_setup"] = (
            (df["close"] > df["donch_hi"])
            & (df["trend_up"] == 1)
            & (df["adx"] >= self.adx_min)
            & (df["rsi"] <= self.rsi_long_max)
        )
        df["short_setup"] = (
            (df["close"] < df["donch_lo"])
            & (df["trend_down"] == 1)
            & (df["adx"] >= self.adx_min)
            & (df["rsi"] >= (100 - self.rsi_short_min))
        )
        return df

    def signal(self, row: pd.Series):
        if bool(row.get("long_setup", False)):
            return 1
        if bool(row.get("short_setup", False)):
            return -1
        return 0


# --------------------------------------------------------------------
# Strategy 2: Turtle-style trend breakout (TrendTurtleV2)
# --------------------------------------------------------------------
class TrendTurtleV2:
    """
    אסטרטגיית טרנד־פולואינג בסגנון Turtle:

    - טרנד עולה: מחיר מעל EMA של ה-HTF
    - טרנד יורד: מחיר מתחת EMA של ה-HTF
    - לונג:  פריצת Donchian High (עם shift) בתוך טרנד עולה
    - שורט:  שבירת Donchian Low (עם shift) בתוך טרנד יורד
    - אפשרות להשתמש ב-ADX למניעת כניסות בריינג'
    """

    def __init__(
        self,
        donchian_len: int = 20,
        ema_len: int = 200,
        adx_len: int = 14,
        adx_min: float = 15,
        use_adx: bool = True,
    ):
        self.dlen = int(donchian_len)
        self.ema_len = int(ema_len)
        self.adx_len = int(adx_len)
        self.adx_min = float(adx_min)
        self.use_adx = bool(use_adx)

    def prepare(self, df_ltf: pd.DataFrame, df_htf: pd.DataFrame):
        df = df_ltf.copy()

        # Donchian
        df["donch_hi"] = df["high"].rolling(self.dlen).max()
        df["donch_lo"] = df["low"].rolling(self.dlen).min()

        # ADX (אופציונלי)
        if self.use_adx:
            df["adx"] = adx(df, self.adx_len)
        else:
            df["adx"] = np.nan

        # HTF trend proxy (EMA)
        htf_ema = df_htf["close"].ewm(span=self.ema_len, adjust=False).mean()
        htf_ema = htf_ema.reindex(df.index, method="ffill")

        df["trend_up"] = (df["close"] > htf_ema).astype(int)
        df["trend_down"] = (df["close"] < htf_ema).astype(int)

        # Breakout ביחס לנר קודם (shift) כדי להימנע מרעש
        donch_hi_prev = df["donch_hi"].shift(1)
        donch_lo_prev = df["donch_lo"].shift(1)

        cond_trend_up = df["trend_up"] == 1
        cond_trend_down = df["trend_down"] == 1

        if self.use_adx:
            cond_adx = df["adx"] >= self.adx_min
        else:
            cond_adx = pd.Series(True, index=df.index)

        df["long_setup"] = (df["close"] > donch_hi_prev) & cond_trend_up & cond_adx
        df["short_setup"] = (df["close"] < donch_lo_prev) & cond_trend_down & cond_adx

        return df

    def signal(self, row: pd.Series):
        if bool(row.get("long_setup", False)):
            return 1
        if bool(row.get("short_setup", False)):
            return -1
        return 0


# --------------------------------------------------------------------
# Strategy 3: Mean-reversion with RSI + Bollinger (MeanReversionRSI)
# --------------------------------------------------------------------
class MeanReversionRSI:
    """
    אסטרטגיית מין-ריברז'ן אינטרדיי:

    - משתמשת ב-RSI + Bollinger Bands
    - טרנד עליון: "קנייה בדיפ" כשהמחיר מתחת לרצועה התחתונה + RSI נמוך
    - טרנד יורד: "מכירה בקפיצה" כשהמחיר מעל הרצועה העליונה + RSI גבוה
    """

    def __init__(
        self,
        rsi_len: int = 14,
        rsi_long_max: float = 35,
        rsi_short_min: float = 65,
        bb_len: int = 20,
        bb_k: float = 2.0,
        ema_len: int = 200,
    ):
        self.rsi_len = int(rsi_len)
        self.rsi_long_max = float(rsi_long_max)
        self.rsi_short_min = float(rsi_short_min)
        self.bb_len = int(bb_len)
        self.bb_k = float(bb_k)
        self.ema_len = int(ema_len)

        # תאימות ל-run_live_week (fallback Donchian)
        self.dlen = self.bb_len

    def prepare(self, df_ltf: pd.DataFrame, df_htf: pd.DataFrame):
        df = df_ltf.copy()

        # RSI
        df["rsi"] = rsi(df["close"], self.rsi_len)

        # Bollinger Bands
        ma = df["close"].rolling(self.bb_len).mean()
        std = df["close"].rolling(self.bb_len).std(ddof=0)
        df["bb_mid"] = ma
        df["bb_up"] = ma + self.bb_k * std
        df["bb_lo"] = ma - self.bb_k * std

        # HTF trend proxy (EMA)
        htf_ema = df_htf["close"].ewm(span=self.ema_len, adjust=False).mean()
        htf_ema = htf_ema.reindex(df.index, method="ffill")
        df["trend_up"] = (df["close"] > htf_ema).astype(int)
        df["trend_down"] = (df["close"] < htf_ema).astype(int)

        # Mean-reversion conditions
        df["long_setup"] = (
            (df["trend_up"] == 1)
            & (df["close"] < df["bb_lo"])
            & (df["rsi"] <= self.rsi_long_max)
        )

        df["short_setup"] = (
            (df["trend_down"] == 1)
            & (df["close"] > df["bb_up"])
            & (df["rsi"] >= self.rsi_short_min)
        )

        return df

    def signal(self, row: pd.Series):
        if bool(row.get("long_setup", False)):
            return 1
        if bool(row.get("short_setup", False)):
            return -1
        return 0


# --------------------------------------------------------------------
# Strategy registry / factory
# --------------------------------------------------------------------
STRATEGY_REGISTRY = {
    "DONCHIAN_ADX_RSI": DonchianTrendADXRSI,
    "TURTLE_TREND_V2": TrendTurtleV2,
    "MEAN_REVERSION_RSI": MeanReversionRSI,
}


def get_strategy_class(name: str):
    """
    מחזיר את קלאס הסטרטגיה לפי שם.
    אם לא נמצא – נופל חזרה ל-DONCHIAN_ADX_RSI כדי לא לשבור את הבוט.
    """
    if not name:
        return DonchianTrendADXRSI
    return STRATEGY_REGISTRY.get(name.upper(), DonchianTrendADXRSI)
