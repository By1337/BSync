Инструкция по ключам для сервера BSync

В этой папке находятся два файла:

* `default` — приватный ключ (НИКОМУ не давайте)
* `default.pub` — публичный ключ (можно передавать)

Эти ключи используются для авторизации на сервере BSync, примерно как логин по паролю:

* `default` — это ваш “пароль”
* `default.pub` — публичная часть, по которой сервер узнаёт вас

Что делать:

1. Скопируйте только `default.pub` в папку `authorized_keys` на сервере BSync.
2. При необходимости переименуйте `default.pub` (это безопасно).
3. `default.pub` не является секретным файлом и может передаваться другим.
4. Внимание: если кто-то получит файл `default` (без `.pub`), он сможет авторизоваться на сервере от вашего имени. Храните этот файл в надёжном месте и никому не передавайте.

---

BSync Server Key Instructions

This folder contains two files:

* `default` — private key (NEVER share this file)
* `default.pub` — public key (safe to share)

These keys are used to authenticate with the BSync server, similar to a password login:

* `default` — your “password”
* `default.pub` — the public part used by the server to identify you

What to do:

1. Copy only `default.pub` into the `authorized_keys` folder on the BSync server.
2. You may rename `default.pub` if needed (this is safe).
3. `default.pub` is not a secret file and can be shared.
4. Warning: if someone gets access to the `default` file (without `.pub`), they will be able to authenticate on the server as you. Keep this file secure and never share it.
