@echo off
REM Build and Run Containers

REM Nom du container Docker
set CONTAINER_NAME=Spark_Master

REM Chemin de destination dans le container
set CONTAINER_SCRIPT_DIR=/spark-scripts
set CONTAINER_SCRIPT_PATH=%CONTAINER_SCRIPT_DIR%/spark_transformations.py

REM Construction des conteneurs
docker-compose build

REM Démarrage des conteneurs en arrière-plan
docker-compose up -d

REM Attente que le conteneur soit prêt (optionnel - vous pouvez améliorer avec un healthcheck ou un timeout)
timeout /t 10 > nul

REM Exécution du script Spark
echo Exécution du script Spark...
docker exec -it %CONTAINER_NAME% /opt/bitnami/spark/bin/spark-submit ^
    --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" ^
    --conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC" ^
    --conf "spark.driver.memory=2g" ^
    --conf "spark.executor.memory=2g" ^
    --conf "spark.logLevel=DEBUG" ^
    %CONTAINER_SCRIPT_PATH%

REM Vérification du code de retour
IF %ERRORLEVEL% NEQ 0 (
    echo Échec de l'exécution du script Spark
    exit /b 1
)

echo Script exécuté avec succès.
exit /b 0
