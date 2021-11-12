package de.hpi.data_preparation.socrata.crawl

object CrawlSummary extends App {

  val summarizer = new CrawlSummarizer(args(0))
  summarizer.allTimeChangeSummary()
}
