# tcache2

## data2db manager

Менеджер запускается по крону в моменты когда необходим сброс данных в БД.

Последовательно перебираются очереди на: добавление (insert)/изменение (update)/удаление (delete) туров
в базе данных.

Каждая обработка insert/update/delete выполняется строго одна за другой. Для обеспечения этого
предназначены флаги TourInsertThreadDataCounter, TourUpdateThreadDataCounter, TourDeleteThreadDataCounter,
которые записываются в Redis.

В начале работы все эти влаги удалены. Когда менеджер загружает в очереди db-воркеров все данные, он
устанавливает значение 0 в соответствующий флаг. Когда у db-воркера заканчиваются данные в его очереди, он\
проверяет наличие этого флага. Если флаг есть, db-воркер увеличивает его на 1 и заканчивает свою работу.

Менеджер по окончанию загрузки данных периодически проверяет не равно-ли значение в флаге количеству воркеров.
Если оно равно - происходит завершение работы.
