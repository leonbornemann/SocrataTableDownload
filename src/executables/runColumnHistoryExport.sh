#setup logs
mkdir parallelLogs
mkdir logs/columnHistoryExport/

#purge logs:
rm parallelLogs/columnHistoryExport
rm logs/columnHistoryExport/*
cat uniqueSocrataIdsEntirePeriod.txt | parallel --joblog parallelLogs/columnHistoryExport --delay 1 --eta -j12 -v 'java -ea -Xmx32g -cp wikixmlsplit-evaluation-cells-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.hpi.data_preparation.socrata.column_history_export.ColumnHistoryExportMain /san2/data/change-exploration/socrata/ {} /san2/data/change-exploration/temporalIND/columnHistories/socrata > logs/columnHistoryExport/{/.}.log 2>&1'
