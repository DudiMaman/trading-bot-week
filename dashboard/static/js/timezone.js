// static/js/timezone.js

// פונקציה לעדכון כל זמני התצוגה לפי הבחירה הנוכחית ב-dropdown
function updateTimesDisplay() {
    var select = document.getElementById('timezoneSelect');
    var tzValue = select.value;  // "IL" או "UTC"
    var tzName = (tzValue === 'IL') ? 'Asia/Jerusalem' : 'UTC';

    // עדכון כל תאי הזמן בטבלה
    var timeCells = document.querySelectorAll('.timestamp');
    timeCells.forEach(function(element) {
        // קבלת הזמן המקורי ב-UTC מה-data attribute
        var utcTimeStr = element.getAttribute('data-utc');
        if (!utcTimeStr) return;
        // הבטחת פורמט parse נכון עבור Date:
        // אם המחרוזת ללא ציון אזור זמן, נוסיף "Z" לסימון UTC (ואם חסר 'T' בין תאריך לשעה, נוסיף).
        if (!utcTimeStr.endsWith('Z')) {
            if (utcTimeStr.indexOf('T') === -1) {
                // החלפת הרווח בין תאריך לשעה ב-T (ISO 8601)
                utcTimeStr = utcTimeStr.trim().replace(' ', 'T');
            }
            // הוספת Z אם אין אופסט בסוף
            if (!/[+-]\d\d:?\\d\d$/.test(utcTimeStr)) {
                utcTimeStr += 'Z';
            }
        }
        var dateObj = new Date(utcTimeStr);
        // המרת התאריך/שעה לאזור הזמן הנבחר בפורמט YYYY-MM-DD HH:MM:SS
        var formattedStr = dateObj.toLocaleString('sv-SE', { timeZone: tzName });
        element.textContent = formattedStr;
    });

    // עדכון תוויות הטקסט (כותרות) לציון אזור הזמן
    var tzLabel = document.getElementById('tzLabel');
    var tzLabelTable = document.getElementById('tzLabelTable');
    if (tzLabel) tzLabel.textContent = (tzValue === 'IL') ? 'IL time' : 'UTC';
    if (tzLabelTable) tzLabelTable.textContent = (tzValue === 'IL') ? 'IL' : 'UTC';
}

// הרצת עדכון ראשוני בעת עליית הדף (כדי להמיר מיידית ל-IL time כברירת מחדל)
document.addEventListener('DOMContentLoaded', function() {
    updateTimesDisplay();
    // האזנה לשינוי ערך ה-dropdown
    var select = document.getElementById('timezoneSelect');
    select.addEventListener('change', updateTimesDisplay);
});
