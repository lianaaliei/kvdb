# kvdb

یک موتور ذخیره‌سازی کلید-مقدار از نوع **LSM-tree** به زبان Rust، بدون هیچ وابستگی خارجی، با سرور سازگار با پروتکل **Redis (RESP)**.

## معماری
- **WAL** با CRC32: هر نوشتن اول اینجا می‌رود؛ دنبالهٔ نیمه‌نوشته بعد از crash شناسایی و حذف می‌شود.
- **Memtable** (BTreeMap) → وقتی پر شد به **SSTable** تبدیل می‌شود (نوشتن tmp + rename اتمیک).
- **SSTable**: داده مرتب + ایندکس پراکنده + **Bloom filter** + footer با magic number.
- **Compaction**: merge استریمی k-way روی همهٔ جدول‌ها، حذف tombstoneها.
  امن در برابر crash: خروجی جایگزین جدیدترین فایل می‌شود و `covers_from` در footer ذخیره می‌شود تا
  موقع بالا آمدن، کامپکشن نیمه‌کاره تکمیل شود (کلید حذف‌شده زنده نمی‌شود).
- **سرور TCP**: thread-per-connection، پشتیبانی از pipelining، دستورات `PING SET GET DEL EXISTS INFO FLUSH COMPACT`.

## اجرا
    cargo run --release -- --dir ./data --addr 127.0.0.1:6380
    redis-cli -p 6380 set hello world

## تست
    cargo test --release   # شامل model-check تصادفی در برابر BTreeMap، تست crash و WAL خراب

## قدم‌های بعدی (ایده)
Leveled compaction، block cache، snapshot/iterator و SCAN، async با tokio، MANIFEST فایل.
