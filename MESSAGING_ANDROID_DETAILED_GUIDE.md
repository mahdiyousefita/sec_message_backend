# راهنمای کامل پیاده‌سازی پیام‌رسان Socket در کلاینت اندروید

این سند نسخه‌ی کامل‌تر برای تیم موبایل است و تمام جریان‌های لازم را مرحله‌به‌مرحله توضیح می‌دهد.

---

## 1) تصویر کلی معماری

جریان پیام در این پروژه دو بخش دارد:

1. **آپلود attachment با HTTP**
   - endpoint: `POST /api/messages/attachments`
   - خروجی: آبجکت `attachment` شامل `type`, `url`, `mime_type`, `file_name`, `size_bytes`, `object_name`

2. **ارسال payload پیام با Socket.IO**
   - event: `send_message`
   - payload شامل `to`, `encrypted_key`, `message` و (اختیاری) `attachment`

> نکته: ارسال HTTP مستقیم پیام (`POST /api/messages/send`) دیگر deprecated است و 410 برمی‌گرداند.

---

## 2) پیش‌نیازهای سرور

- کاربر باید قبلاً ثبت‌نام/لاگین کرده باشد.
- توکن معتبر JWT access token نیاز است.
- Redis باید در دسترس باشد (برای پیام‌های آفلاین).
- Socket.IO endpoint باید از طریق Nginx/Proxy قابل دسترس باشد (`/socket.io/`).

---

## 3) قرارداد API و Eventها

### 3.1) HTTP Upload Attachment

`POST /api/messages/attachments`

Headers:
- `Authorization: Bearer <access_token>`

Body:
- `multipart/form-data`
- فیلد فایل: `file` (یا `attachment`)

Response (201):

```json
{
  "attachment": {
    "type": "image",
    "mime_type": "image/webp",
    "file_name": "photo.webp",
    "size_bytes": 10240,
    "object_name": "messages/alice/uuid.webp",
    "url": "https://api.example.com/media/messages/alice/uuid.webp"
  }
}
```

خطاهای مهم:
- `400` نوع فایل نامعتبر یا فایل خالی
- `503` اختلال storage

---

### 3.2) Socket اتصال و احراز هویت

در connect باید token را بدهید:

```json
{"token": "<access_token>"}
```

در اتصال موفق، سرور eventهای زیر را می‌دهد:
- `connected`
- `pending_messages` (اگر پیام آفلاین داشته باشید)

---

### 3.3) Socket ارسال پیام

Event ارسالی:
- `send_message`

Payload حداقلی متن:

```json
{
  "to": "bob",
  "type": "text",
  "message": "<encrypted_message>",
  "encrypted_key": "<encrypted_key>"
}
```

Payload برای attachment:

```json
{
  "to": "bob",
  "type": "image",
  "message": "<encrypted_caption_or_null>",
  "encrypted_key": "<encrypted_key>",
  "attachment": {
    "type": "image",
    "mime_type": "image/webp",
    "file_name": "photo.webp",
    "size_bytes": 10240,
    "url": "https://api.example.com/media/messages/alice/uuid.webp"
  }
}
```

Eventهای دریافتی:
- برای sender: `message_sent`
- برای receiver: `new_message`
- خطا: `message_error`

---

### 3.4) Presence

- استعلام دستی:
  - emit: `get_user_status` با payload `{ "username": "bob" }`
- دریافت پاسخ/تغییرات:
  - event: `user_status` با payload:

```json
{ "username": "bob", "online": true }
```

---

## 4) ترتیب درست پیاده‌سازی در کلاینت (Recommended Flow)

1. Login و گرفتن `access_token`
2. Connect Socket با `auth.token`
3. Listen برای eventهای:
   - `connected`
   - `pending_messages`
   - `new_message`
   - `message_sent`
   - `message_error`
   - `user_status`
4. برای متن ساده:
   - مستقیم `send_message`
5. برای attachment:
   - اول `POST /api/messages/attachments`
   - بعد payload خروجی attachment را داخل `send_message` بفرست

---

## 5) نکات مهم رمزنگاری (Client Responsibility)

Backend فقط payload را عبور می‌دهد/صف می‌کند. مسئولیت رمزنگاری سمت کلاینت است:

- `message` باید ciphertext باشد (یا caption رمز‌شده)
- `encrypted_key` برای decrypt کردن payload روی گیرنده استفاده می‌شود
- حتی برای attachment بهتر است متادیتای حساس را رمزنگاری کنید (با توجه به مدل امنیتی شما)

---

## 6) Edge Caseها که حتماً هندل کنید

1. `message_error: Unauthorized`
   - توکن منقضی شده یا اتصال با auth اشتباه
2. `message_error: Recipient not found`
   - username مقصد اشتباه است
3. `message_error: Message or attachment is required`
   - payload خالی است
4. `message_error: Invalid attachment payload`
   - ساختار `attachment` صحیح نیست
5. `pending_messages`
   - هنگام reconnect باید merge با local state درست انجام شود

---

## 7) Troubleshooting سریع برای تیم موبایل

### 7.1) Timeout روی Socket

- چک کنید URL درست باشد (`https://api.dinosocial.ir`)
- `socket.io path` پیش‌فرض `socket.io` باشد
- `transports` را در شروع روی `websocket` یا fallback polling تنظیم کنید
- اگر پشت شبکه محدود هستید، polling fallback را فعال کنید

### 7.2) `message_sent` می‌گیرید اما `new_message` نمی‌رسد

- ممکن است گیرنده آنلاین نباشد؛ باید پیام در `pending_messages` بعدی برسد
- گیرنده باید واقعاً Socket connected باشد

### 7.3) آپلود attachment خطا می‌دهد

- MIME type فایل را چک کنید
- سقف حجم با `MESSAGE_ATTACHMENT_MAX_SIZE_BYTES` کنترل می‌شود
- اگر storage مشکل داشته باشد 503 برمی‌گردد

---

## 8) Kotlin Skeleton (Production-friendly)

```kotlin
class MessagingSocketClient(
    private val baseUrl: String,
    private val tokenProvider: () -> String,
) {
    private var socket: Socket? = null

    fun connect() {
        val opts = IO.Options.builder()
            .setAuth(mapOf("token" to tokenProvider()))
            .setTransports(arrayOf("websocket"))
            .setReconnection(true)
            .setReconnectionAttempts(Int.MAX_VALUE)
            .setReconnectionDelay(1000)
            .setReconnectionDelayMax(10000)
            .build()

        socket = IO.socket(baseUrl, opts).apply {
            on(Socket.EVENT_CONNECT) { /* connected */ }
            on("connected") { args -> /* server ack */ }
            on("pending_messages") { args -> /* load queue */ }
            on("new_message") { args -> /* add incoming */ }
            on("message_sent") { args -> /* sender ack */ }
            on("message_error") { args -> /* show/log error */ }
            on("user_status") { args -> /* presence updates */ }
            connect()
        }
    }

    fun sendText(to: String, encryptedMessage: String, encryptedKey: String) {
        val payload = JSONObject()
            .put("to", to)
            .put("type", "text")
            .put("message", encryptedMessage)
            .put("encrypted_key", encryptedKey)

        socket?.emit("send_message", payload)
    }

    fun sendAttachment(
        to: String,
        type: String,
        encryptedKey: String,
        attachment: JSONObject,
        encryptedCaption: String? = null,
    ) {
        val payload = JSONObject()
            .put("to", to)
            .put("type", type)
            .put("encrypted_key", encryptedKey)
            .put("attachment", attachment)
            .put("message", encryptedCaption)

        socket?.emit("send_message", payload)
    }

    fun queryUserStatus(username: String) {
        socket?.emit("get_user_status", JSONObject().put("username", username))
    }

    fun disconnect() {
        socket?.disconnect()
        socket?.off()
        socket = null
    }
}
```

---

## 9) چک‌لیست QA قبل از release کلاینت

- [ ] text message ارسال/دریافت آنلاین
- [ ] attachment message ارسال/دریافت آنلاین
- [ ] گیرنده آفلاین → دریافت در `pending_messages`
- [ ] reconnect بعد از قطع شبکه
- [ ] refresh token و reconnect socket
- [ ] presence (`user_status`) برای userهای تست
- [ ] handling خطاهای `message_error`

---

## 10) نکات نهایی

- در صورت multi-instance backend، presence مبتنی بر in-memory map ممکن است cross-instance کامل نباشد.
- برای UX بهتر، presence را همیشه best-effort در نظر بگیرید نه truth قطعی.
- اگر timeout دارید، اول با curl endpointها را تست کنید تا backend/network از client issue جدا شود.

