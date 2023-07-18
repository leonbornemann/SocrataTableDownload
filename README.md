# SocrataTableDownload

Repository to automatically download datasets from Socrata, an open government data portal (https://api.us.socrata.com/). Subsequently, the program compares new tables with previous versions and stores diffs to compress the history.
Executing [SocrataCrawlMain](src/main/scala/de.hpi/data_preparation/socrata/crawl/SocrataCrawlMain.scala) will start the crawler that will then download all datasets that are currently published, check them for changes with previously downloaded versions of the same datasets and store the changes as diffs.
