# airflow_spark_postres

1) установить docker/docker-compose в систему
2) будучи в папке docker/docker-airflow в консоли прописать "docker build --rm -t docker-airflow-spark:latest ." , начнётся сборка билда самого airflow
3) будучи в папке docker прописать в консоли "docker-compose up" ; начнётся сборка контейнера 

з.ы. во время запуска у меня частенько падал сервер постгреса, помогает вручную его перезапустить в самом docker desktop

Пути:

1) dags - папка для скриптов дагов
2) logs - логи, внезапно
3) spark\app - папка для скриптов спарка

Airflow: http://localhost:8282 

Spark Master: http://localhost:8181



если всё ок, то можно зайти в Admin/Connections в админке airflow, и настроить коннект к спарку и запустить DAG spark-test (сначала активировать его, а потом тригернуть)
![airflow_spark_connection](https://user-images.githubusercontent.com/13750064/125505227-33a8eb21-1877-4ead-86dc-4f30a694d6bb.png)
