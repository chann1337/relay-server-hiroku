# Hiroku Relay Server

WebSocket relay сервер для обхода Symmetric NAT в России.
Работает на Render Free Tier без привязки карты.

## Деплой на Render (5 минут)

1. Создай новый репозиторий на GitHub:
   ```bash
   cd relay_server
   git init
   git add .
   git commit -m "Initial commit: Hiroku Relay Server"
   git remote add origin https://github.com/ТВОЙ_USERNAME/hiroku-relay.git
   git push -u origin main
   ```

2. Зайди на https://render.com → Sign Up (GitHub)

3. Dashboard → New + → Web Service

4. Подключи репозиторий `hiroku-relay`

5. Настройки:
   - **Name**: `hiroku-relay`
   - **Environment**: `Python`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python server.py`
   - **Instance Type**: `Free`

6. Нажми **Create Web Service**

7. После деплоя скопируй URL (вида `https://hiroku-relay-xxxx.onrender.com`)

8. Замени в `RelayViewModel.kt`:
   ```kotlin
   private val relayServerUrl: String = "wss://hiroku-relay-xxxx.onrender.com"
   ```

## Настройка UptimeRobot (чтобы сервер не засыпал)

1. Зайди на https://uptimerobot.com → Sign Up (бесплатно)

2. Dashboard → Add New Monitor

3. Настройки:
   - **Monitor Type**: `HTTP(s)`
   - **Friendly Name**: `Hiroku Relay`
   - **URL**: `https://hiroku-relay-xxxx.onrender.com`
   - **Monitoring Interval**: `5 minutes`

4. Нажми **Create Monitor**

Готово! Сервер будет пинговаться каждые 5 минут и не заснёт.

## Архитектура

```
[Хост Android] ←WebSocket→ [Render Server:8765] ←WebSocket→ [Гость Android]
```

- Все данные (сигнализация + игровой трафик) идут через один WebSocket порт
- Бинарные данные игры передаются как WebSocket binary frames
- Работает через Symmetric NAT (не нужен P2P)

## Локальный запуск

```bash
cd relay_server
pip install -r requirements.txt
python server.py
```

Сервер запустится на `ws://localhost:8765`
