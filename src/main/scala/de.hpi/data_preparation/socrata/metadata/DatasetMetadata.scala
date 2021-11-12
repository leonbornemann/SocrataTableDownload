package de.hpi.data_preparation.socrata.metadata

import de.hpi.data_preparation.socrata.JsonReadable

case class DatasetMetadata(resource:Resource,
                           classification:Classification,
                           metadata:AdditionalMetadata,
                           permalink:String,
                           link:String,
                           preview_image_url:Option[String],
                           owner:User,
                           published_copy:Option[Any])

object DatasetMetadata extends JsonReadable[DatasetMetadata]