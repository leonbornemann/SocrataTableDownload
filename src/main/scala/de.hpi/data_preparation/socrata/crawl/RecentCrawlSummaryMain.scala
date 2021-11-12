package de.hpi.data_preparation.socrata.crawl

object RecentCrawlSummaryMain extends App{
  val summarizer = new CrawlSummarizer(args(0),Some(args(1)))
  summarizer.recentCrawlSummary()

}
