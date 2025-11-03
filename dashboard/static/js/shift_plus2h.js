// תצוגה בלבד: מזיז +2 שעות לכל הזמנים המגיעים מ-/data, בלי לשנות עיצוב/קוד קיים.
(function () {
  const SHIFT_MS = 2 * 60 * 60 * 1000; // +2h
  const _fetch = window.fetch;

  function toDate(v){ if(!v) return null; const d=new Date(v); return isNaN(d.getTime())?null:d; }
  function plus2Iso(v){ const d = toDate(v); return d ? new Date(d.getTime()+SHIFT_MS).toISOString() : null; }

  window.fetch = async function(input, init){
    const res = await _fetch(input, init);
    try{
      const url = (typeof input==='string') ? input : (input && input.url) || '';
      if(url.includes('/data')){
        const clone = res.clone();
        const data  = await clone.json();

        // Last refresh
        const baseNow = data.now_utc || data.now || data.server_time;
        const shiftedNow = plus2Iso(baseNow);
        if(shiftedNow){
          data.now_utc = shiftedNow;
          data.now_utc_simple = shiftedNow.replace('T',' ').replace('Z','');
          data.now_il = shiftedNow;
        }

        // Trades
        if(Array.isArray(data.trades)){
          data.trades = data.trades.map(r=>{
            const base = r.time || r.timestamp || r.ts || r.datetime;
            const s = plus2Iso(base);
            return s ? { ...r, time: s, time_il: s } : r;
          });
        }

        // Equity
        if(Array.isArray(data.equity)){
          data.equity = data.equity.map(r=>{
            const base = r.time || r.timestamp || r.ts || r.datetime;
            const s = plus2Iso(base);
            return s ? { ...r, time: s, time_il: s } : r;
          });
        }

        // מחזירים תשובה מעודכנת לתצוגה בלבד
        const blob = new Blob([JSON.stringify(data)], { type: 'application/json' });
        return new Response(blob, { status: res.status, statusText: res.statusText, headers: res.headers });
      }
    }catch(e){ /* במקרה חריג – לא משנים את התגובה */ }
    return res;
  };
})();
