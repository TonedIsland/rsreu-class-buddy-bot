# Telegram Bot для расписания РГРТУ

## Установка на сервер

1. Клонировать репозиторий
2. Создать виртуальное окружение:
python3 -m venv venv
source venv/bin/activate

3. Установить зависимости:

pip install -r requirements.txt

4. Создать файл `.env` на основе `.env.example` и заполнить токены
5. Запустить бота:

python3 main.py


## Автозапуск (systemd)

Создать файл `/etc/systemd/system/telegram-bot.service`:

[Unit]
Description=Telegram Bot
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/home/your_username/telegram-bot
Environment="PATH=/home/your_username/telegram-bot/venv/bin"
ExecStart=/home/your_username/telegram-bot/venv/bin/python3 main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target


Запустить:

sudo systemctl enable telegram-bot
sudo systemctl start telegram-bot