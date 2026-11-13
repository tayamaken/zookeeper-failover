# ZooKeeper: отказоустойчивый расчёт пар навыков

Ансамбль из трёх серверов ZooKeeper 3.9.6 и два Python-координатора подсчёта пар навыков из ДЗ № 3. Нормализация и исходный эталон взяты из [java-vacancy-analysis](https://github.com/tayamaken/java-vacancy-analysis). Происхождение файлов и SHA256 записаны в `baseline/source.json`.

## Запуск

Нужны Docker Desktop с Linux-контейнерами и Python 3.10+. Java устанавливается внутри образа ZooKeeper; локальная установка для Docker-варианта не требуется.

Из каталога репозитория:

```powershell
python scripts/prepare_inputs.py
python scripts/run_experiments.py
python scripts/verify_results.py
```

По умолчанию CSV копируется из `../java-vacancy-analysis/data/java_vacancies.csv`. Для другого расположения:

```powershell
python scripts/prepare_inputs.py --source D:\path\java_vacancies.csv
```

Исходный CSV и рабочие чекпоинты исключены из Git. Сохранённые результаты можно проверить через `verify_results.py` без исходного CSV и Docker. Новый запуск создаёт отдельные каталоги с меткой времени UTC.

## Команды для ручного запуска

```powershell
docker compose build receiver
docker compose up -d --wait zk1 zk2 zk3 receiver
docker compose exec -T receiver python src/probe.py roles
$env:RUN_ID = "manual-unfenced-1"
$env:FENCING = "0"
docker compose up -d --no-deps --force-recreate coordinator-a coordinator-b
docker compose logs -f coordinator-a coordinator-b
```

В другом терминале после `checkpoint_saved` приостановить координатор, в логе которого есть `became_leader`. Пример для `coordinator-a`:

```powershell
docker compose pause coordinator-a
# Дождаться публикации результата запасным и выдержать паузу минимум 40 секунд.
docker compose unpause coordinator-a
docker compose exec -T receiver python src/probe.py snapshot --run manual-unfenced-1
```

Для защищённого сценария задать новый `RUN_ID` и `FENCING=1`, затем повторить запуск. Тайм-аут сессии — 10 секунд. Задержка 0,5 секунды на 200 строк нужна для воспроизводимой приостановки посреди расчёта.

Порты клиентов на хосте: `127.0.0.1:2181`, `:2182`, `:2183`. У каждого сервера отдельные Docker-тома `/data` и `/datalog`; `myid` создаёт entrypoint официального образа.

## Файлы

- `compose.yaml` — конфигурация ансамбля и приложения.
- `src/coordinator.py` — выбор лидера через ephemeral sequential lock, расчёт и восстановление.
- `src/receiver.py` — получатель публикаций; при `FENCING=1` проверяет версию эпохи и меняет указатель одной транзакцией ZooKeeper.
- `scripts/run_experiments.py` — проверки кворума, реального `zkCli` и двух сценариев паузы лидера.
- `logs/commands.txt` — команды с выводом и кодами завершения.
- `logs/zkcli-owner.txt` — отдельная сессия `zkCli`, создавшая эфемерный узел.
- `logs/<run>/` — JSONL-журналы обоих координаторов и получателя с UTC-временем.
- `logs/zk*-config.txt`, `logs/zk*.log` — фактические конфигурации, myid и журналы серверов.
- `results/<run>/` — CSV-кандидаты, итоговый указатель ZooKeeper и автоматические проверки.
- `results/verification.json` — машинные результаты экспериментов, включая время смены лидера.

CSV сохраняются как неизменяемые кандидаты. Опубликованным результатом считается только объект, на который ссылается znode `result`; отклонённый кандидат не меняет этот указатель. Чекпоинт состоит из файла со счётчиками и количества обработанных логических CSV-строк, а его указатель обновляется с проверкой эпохи в обоих сценариях. Возобновлённый устаревший процесс намеренно пытается опубликовать результат, чтобы проверить защиту на стороне получателя.

Завершить работу контейнеров, сохранив данные:

```powershell
docker compose down
```

Отчёт не входит в репозиторий.

## Документация

- [Официальный Docker-образ ZooKeeper](https://hub.docker.com/_/zookeeper)
- [ZooKeeper 3.9.6 Programmer's Guide](https://zookeeper.apache.org/doc/r3.9.6/zookeeperProgrammers.html)
- [Kazoo: блокировки и состояния сессий](https://kazoo.readthedocs.io/en/latest/api/recipe/lock.html)
