/*********************************************************************************************************************************
* Simple object to load data file(s) as a dataframe and submit the appropriate df column through Apache Tika for text extraction
* Case matching for output used for easy data mining on exceptions and later analysis
**********************************************************************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

import org.apache.tika.parser.ocr.TesseractOCRConfig
import org.apache.tika.parser.pdf.PDFParserConfig
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser}
import org.apache.tika.sax.BodyContentHandler

import org.xml.sax.SAXException

object Driver {
  def main(args: Array[String]):Unit = {
    
    val spark_conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName("Spark Tika HDFS")
    val sc = new SparkContext(spark_conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    // Put your input to dataframe here
    val df = hiveContext.read.json("hdfs://HOSTNAME:PORT/path/to/data/in/*")
    
    
    // Function literal using Apache Tika and Tesseract to extract text from a byte array (doc)
    val extractInfo: (Array[Byte] => String) = (fp: Array[Byte]) => {

      try {
        val parser:Parser = new AutoDetectParser()
        val handler:BodyContentHandler = new BodyContentHandler(Integer.MAX_VALUE)
        val config:TesseractOCRConfig  = new TesseractOCRConfig()
        val pdfConfig:PDFParserConfig = new PDFParserConfig()
        pdfConfig.setExtractInlineImages(true)

        val inputstream:InputStream = new ByteArrayInputStream(fp)

        val metadata:Metadata = new  Metadata()
        val parseContext:ParseContext = new ParseContext()
        parseContext.set(classOf[TesseractOCRConfig], config)
        parseContext.set(classOf[PDFParserConfig], pdfConfig)
        parseContext.set(classOf[Parser], parser)
        parser.parse(inputstream, handler, metadata, parseContext)
        handler.toString
        } catch {
          case e: TikaException => "TIKAEXCEPTION"
          case e: SAXException => "SAXEXCEPTION"
        }
    }


    val extract_udf = udf(extractInfo)


    /**************************************************************************************************
    * Create a dataframe that replaces the base64 encoded string document with a byte array (binary file)
    * Assumes file is base64 encoded string.  Could also use paths if files are accessible, but if you want
    * to use streaming then this will depend
    *****************************************************************************************************/
    val df2 = df.withColumn("unbased_media", unbase64($"media_file")).drop("media_file")

    // Another dataframe that replaces the byte array doc with the extracted text of the doc
    val dfRenamed = df2.withColumn("media_corpus", extract_udf(col("unbased_media"))).drop("unbased_media")
  }
}
