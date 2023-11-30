# AntiFraud-system
ETL процесс по отлову мошеннических транзакций  в банке

Организовано хранение таблиц измерений в формате SCD2.
В отдельные скрипты вынесены процессы:
  - создания подключений к базам PostgreSQL;
  - переименование и перемещение файлов-источников в архив.

Созданы проверки:
  - наличие файлов passport_blacklist, terminals, transactions для загрузки, если какой-то из файлов отсутствует - выводится соответствующее 	сообщение и продолжается выполнение скрипта;
  - повторная загрузка файла, если наименование файла предлагаемого для загрузки соответствует наименованию файла загруженного ранее и отправленного в архив, то такой файл будет игнорироваться, данные из него не будут загружены.

Шаги процесса обработки данных
![Рисунок1](https://github.com/MyNameIsAriadna/AntiFraud-system/assets/128482161/d41b385e-2917-4b7a-8a95-fdd32551a88f)
