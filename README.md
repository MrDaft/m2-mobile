# m2-mobile

## Настройка переменных окружения GoogleCloud VM
### Для возможности запуска через терминал: 

* в терминале открываем редактирование `nano ~/.bash_profile`

* добавляем свою переменную `export GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"`, где `GOOGLE_APPLICATION_CREDENTIALS` название вашей переменной, а `KEY_PATH` ключ, который вы хотите передать

**NB**

* Обязательно с новой строки
* не ставить пробелы между до и после знака `=`


### Для возможности запуска через CRON: 
* в терминале открываем редактирование `crontab -e`

* добавляем свою переменную `export GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"`, где `GOOGLE_APPLICATION_CREDENTIALS` название вашей переменной, а `KEY_PATH` ключ, который вы хотите передать

**NB**
* Обязательно в блоке с переменными
* Обязательно с новой строки
* не ставить пробелы до и после знака `=`