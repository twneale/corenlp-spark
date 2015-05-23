import java.util.Properties
import java.io.{ByteArrayOutputStream,StringWriter,PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP

import play.api.libs.json._


object SparkJob {
  def main(args: Array[String]) {
    var props = new Properties
    val annotators = args(0)
    val input = args(1)
    val output = args(2)
    val numpartitions = args(3).toInt

    val conf = new SparkConf()
    conf.setAppName(s"CoreNLP($annotators) - $input - $output")
    val sc = new SparkContext(new SparkConf())

    props.setProperty("annotators", annotators)
    object CoreNLP {
      @transient lazy val pipeline = new StanfordCoreNLP(props)

      def getXML(sentence: String): String = {
          val document = new Annotation(sentence)
          pipeline.annotate(document)
          var output_stream = new ByteArrayOutputStream
          pipeline.xmlPrint(document, output_stream)
          return output_stream.toString("utf8");
        }

      def processJSON(jsonstring: String): String = {
          val json: JsValue = Json.parse(jsonstring)
          val text = (json \ "text").toString()
          val xml = getXML(text)
          var result = json.as[JsObject] - "text"
          result = result.as[JsObject] + ("xml" -> JsString(xml))
          return result.toString()
        }
      }

    var data = sc.textFile(input)
    data = data.repartition(numpartitions)
    val xml = data.map(s => CoreNLP.processJSON(s))
    xml.saveAsTextFile(output)
    }
}
