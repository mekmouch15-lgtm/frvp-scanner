# FRVP Scanner (MEXC → Telegram)

سكريبت بايثون بيراقب 564 عملة على MEXC كل 15 دقيقة، بنفس منطق مؤشر
"FRVP Quant Scalper" الأصلي (Swing على 1H + POC Retest على 15m)، ويبعت
أي صفقة جديدة (BUY / TP / SL) لقناة تليجرام تلقائي عبر GitHub Actions.

## خطوات التركيب (مرة واحدة بس)

1. ارفع الملفات التلاتة دي في الـ repository بتاعك (`frvp-scanner`):
   - `scanner.py`
   - `symbols.txt`
   - `.github/workflows/scan.yml`

2. روح على **Settings → Secrets and variables → Actions** في الـ repository،
   ودوس **New repository secret** مرتين عشان تضيف:
   - `TELEGRAM_BOT_TOKEN` → التوكن بتاع البوت
   - `TELEGRAM_CHAT_ID` → الـ Chat ID بتاع القناة (الرقم اللي بيبدأ بـ `-100`)

3. روح على تبويب **Actions** في أعلى الصفحة، وفعّل الـ Workflows لو طلب منك
   ذلك ("I understand my workflows, go ahead and enable them").

4. السكريبت هيشتغل تلقائي كل 15 دقيقة من دلوقتي. تقدر كمان تشغّله يدوي
   للتجربة: Actions → FRVP Scanner → Run workflow.

## ملاحظات مهمة

- **أول تشغيل لن يرسل أي تنبيهات** — بيسجل بس آخر شمعة معالجة لكل عملة كنقطة
  بداية، عشان محدش يستقبل صفقات قديمة فجأة في القناة.
- من التشغيلة التانية، أي صفقة جديدة هتتبعت تلقائي.
- الملف `state.json` بيتحدث ويتحفظ في الـ repository تلقائي بعد كل تشغيلة
  (عشان السكريبت يتذكر آخر نقطة وصل لها).
- لو عايز تغيّر عدد العملات أو تضيف/تشيل عملة، عدّل ملف `symbols.txt` فقط
  (سطر واحد لكل عملة، بدون بادئة MEXC).
