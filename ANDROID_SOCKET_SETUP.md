# راهنمای ساده اتصال کلاینت اندروید به Socket پیام‌رسان

این راهنما برای این پروژه است تا ارسال/دریافت پیام به‌جای HTTP با Socket.IO انجام شود.

## 1) پیش‌نیاز سرور

- Python dependencies نصب باشد (`requirements.txt`).
- Redis در دسترس باشد (برای پیام‌های آفلاین).
- سرور را با `socketio.run(...)` بالا بیاورید (فایل `run.py` همین کار را می‌کند).

## 2) متغیرهای محیطی مهم (Server Config)

در محیط production این موارد را تنظیم کنید:

- `JWT_SECRET_KEY`: کلید JWT قوی.
- `CORS_ALLOWED_ORIGINS`: لیست originهای مجاز کلاینت (comma-separated).
  - مثال: `https://app.example.com,https://admin.example.com`
- `SOCKETIO_MESSAGE_QUEUE`: برای چند instance سرور، آدرس message queue.
  - مثال با Redis: `redis://127.0.0.1:6379/0`
- `SOCKETIO_PING_TIMEOUT` (پیش‌فرض: 25)
- `SOCKETIO_PING_INTERVAL` (پیش‌فرض: 20)
- `SOCKETIO_LOGGER` و `SOCKETIO_ENGINEIO_LOGGER` برای debug (معمولاً `false` در production)
- `MESSAGE_ATTACHMENT_MAX_SIZE_BYTES` برای سقف حجم فایل ارسالی پیام

> نکته: اگر فقط یک instance دارید، `SOCKETIO_MESSAGE_QUEUE` اجباری نیست؛
> ولی برای scale شدن (چند worker/pod) حتماً تنظیمش کنید.

## 3) روال احراز هویت کلاینت

1. کاربر با HTTP لاگین می‌کند: `POST /api/auth/login`
2. `access_token` را می‌گیرد.
3. هنگام اتصال Socket، token را در `auth` بفرستید:

```json
{"token": "<access_token>"}
```

اگر token معتبر نباشد اتصال reject می‌شود.

## 4) Eventهای Socket

### رویدادهای سرور
- `connected`: بعد از اتصال موفق
- `pending_messages`: پیام‌های صف‌شده آفلاین
- `new_message`: پیام زنده از مخاطب
- `message_sent`: تایید ارسال برای فرستنده
- `message_error`: خطای اعتبارسنجی/احراز هویت
- `user_status`: وضعیت آنلاین/آفلاین کاربران

### رویدادهای ارسالی از کلاینت
- `send_message`
- `get_user_status`

Payload برای `send_message`:
```json
{
  "to": "bob",
  "type": "image",
  "message": "<encrypted_message_or_caption>",
  "encrypted_key": "<encrypted_key>",
  "attachment": {
    "type": "image",
    "url": "https://api.example.com/media/messages/alice/uuid.webp",
    "mime_type": "image/webp",
    "file_name": "chat.webp",
    "size_bytes": 10240
  }
}
```

## 5) نمونه پیاده‌سازی اندروید (Kotlin + socket.io-client)

> کتابخانه رایج: `io.socket:socket.io-client`

Pseudo code:

```kotlin
val opts = IO.Options.builder()
    .setAuth(mapOf("token" to accessToken))
    .setTransports(arrayOf("websocket"))
    .build()

val socket = IO.socket("https://api.example.com", opts)

socket.on(Socket.EVENT_CONNECT) {
    // connected
}

socket.on("connected") { args ->
    // args[0] => {"username":"alice"}
}

socket.on("pending_messages") { args ->
    // messages array
}

socket.on("new_message") { args ->
    // receive live message
}

socket.on("message_sent") { args ->
    // delivery ack for sender
}

socket.on("message_error") { args ->
    // handle error
}

socket.connect()

socket.on("user_status") { args ->
    // {"username":"bob", "online": true/false}
}

fun sendMessage(
    to: String,
    encryptedMessage: String?,
    encryptedKey: String,
    type: String,
    attachment: JSONObject? = null,
) {
    val payload = JSONObject()
        .put("to", to)
        .put("type", type)
        .put("message", encryptedMessage)
        .put("encrypted_key", encryptedKey)

    if (attachment != null) {
        payload.put("attachment", attachment)
    }

    socket.emit("send_message", payload)
}

fun getUserStatus(username: String) {
    socket.emit("get_user_status", JSONObject().put("username", username))
}
```

## 6) رفتار پیام آنلاین/آفلاین

- اگر گیرنده آنلاین باشد: پیام با `new_message` لحظه‌ای می‌رسد و در صف ذخیره نمی‌شود.
- اگر گیرنده آفلاین باشد: پیام در Redis صف می‌شود و اتصال بعدی با `pending_messages` تحویل می‌گیرد.

## 7) نکات استقرار سرور (Production)

- حتماً HTTPS فعال باشد.
- reverse proxy (مثل Nginx) باید WebSocket upgrade را پاس بدهد.
- اگر چند worker/server دارید:
  - `SOCKETIO_MESSAGE_QUEUE` را روی Redis بگذارید.
  - همه instanceها باید به همان queue وصل باشند.
- برای اطمینان از پایداری:
  - timeoutهای ping را طبق شبکه موبایل تنظیم کنید.
  - reconnect strategy سمت کلاینت فعال باشد.

## 8) نکته مهم درباره HTTP پیام

ارسال پیام با `POST /api/messages/send` deprecated شده و `410` می‌دهد.
برای ارسال جدید فقط `send_message` روی Socket استفاده کنید.

## 9) آپلود فایل/عکس/ویدیو/ویس برای پیام

قبل از ارسال پیام socket، فایل را با HTTP آپلود کنید:

- `POST /api/messages/attachments`
- Header: `Authorization: Bearer <access_token>`
- Body: `multipart/form-data` با فیلد `file`

خروجی این endpoint یک آبجکت `attachment` می‌دهد؛ همان آبجکت را داخل `send_message` بفرستید.
