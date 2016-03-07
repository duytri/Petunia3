package main.scala

import scala.swing._
import scala.swing.event._
import javax.swing.ImageIcon
import javax.swing.border.Border
import java.awt.Color
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.evaluation.binary.FMeasure

object Petunia extends SimpleSwingApplication {
  def top = new MainFrame {
    resizable = false
    title = "Chương trình phân lớp văn bản theo chủ đề"
    val iconURL = getClass.getResource("/img/logo.png")
    iconImage = toolkit.createImage(iconURL)

    //~~~~~~~~~~~ Title ~~~~~~~~~~~
    val icon = new Label {
      icon = new ImageIcon(iconURL)
    }
    val lbTitle = new Label {
      text = " CHƯƠNG TRÌNH PHÂN LỚP VĂN BẢN THEO CHỦ ĐỀ"
      font = new Font("Dialog", java.awt.Font.BOLD, 20)
    }
    val lbSubTitle = new Label {
      text = " Phòng thí nghiệm Hệ Thống Thông Tin - Trường Đại học Công Nghệ Thông Tin - ĐHQG TP.HCM"
      font = new Font("Segoe UI Light", java.awt.Font.ITALIC, 14)
    }
    val pnIcon = new BoxPanel(Orientation.Vertical) {
      contents += icon
    }
    val pnTitleCenter = new BoxPanel(Orientation.Vertical) {
      contents += lbTitle
      contents += lbSubTitle
      border = Swing.EmptyBorder(12, 10, 10, 15)
    }
    val pnTitle = new BorderPanel() {
      layout(pnIcon) = BorderPanel.Position.West
      layout(pnTitleCenter) = BorderPanel.Position.Center
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Train ~~~~~~~~~~~
    val lbTrain = new Label {
      text = "CÁC THÔNG SỐ HUẤN LUYỆN"
    }
    val pnTrainR1 = new BorderPanel {
      layout(lbTrain) = BorderPanel.Position.West
    }
    ////~~~~~~~~~~~ Train-row2 ~~~~~~~~~~~
    val lbTrainR2 = new Label {
      text = "Đường dẫn thư mục dữ liệu huấn luyện (input)    "
    }
    val tfTrainDir = new TextField {
      columns = 1
      text = "/home/duytri/Desktop/"
      border = Swing.EmptyBorder(5, 10, 5, 10)
    }
    val pnTrainR2 = new BoxPanel(Orientation.Horizontal) {
      contents += lbTrainR2
      contents += tfTrainDir
      border = Swing.EmptyBorder(10, 0, 0, 0)
    }
    ////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ////~~~~~~~~~~~ Train-row3 ~~~~~~~~~~~
    val lbTrainR31 = new Label {
      text = "Ngưỡng TF*IDF                                   "
    }
    val lbTrainR32 = new Label {
      text = "Ngưỡng dưới  "
    }
    val tfLowerBound = new TextField {
      columns = 1
      text = "0.0006"
      border = Swing.EmptyBorder(5, 10, 5, 10)
    }
    val lbTrainR33 = new Label {
      text = "  Ngưỡng trên  "
    }
    val tfUpperBound = new TextField {
      columns = 1
      text = "0.6"
      border = Swing.EmptyBorder(5, 10, 5, 10)
    }
    val pnTrainR3 = new BoxPanel(Orientation.Horizontal) {
      contents += lbTrainR31
      contents += lbTrainR32
      contents += tfLowerBound
      contents += lbTrainR33
      contents += tfUpperBound
      border = Swing.EmptyBorder(10, 0, 0, 0)
    }
    ////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ////~~~~~~~~~~~ Train-row4 ~~~~~~~~~~~
    val lbTrainR4 = new Label {
      text = "Đường dẫn thư mục lưu trữ mô hình huấn luyện  "
    }
    val tfModelDir = new TextField {
      columns = 1
      text = "/home/duytri/Desktop/"
      border = Swing.EmptyBorder(5, 10, 5, 10)
    }
    val pnTrainR4 = new BoxPanel(Orientation.Horizontal) {
      contents += lbTrainR4
      contents += tfModelDir
      border = Swing.EmptyBorder(10, 0, 0, 0)
    }
    ////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ////~~~~~~~~~~~ Train-row5 ~~~~~~~~~~~
    val lbTrainR5 = new Label {
      text = "Tỷ lệ phân chia dữ liệu huấn luyện                        "
    }
    val tfTrainPercent = new TextField {
      columns = 1
      text = "0.7"
      border = Swing.EmptyBorder(5, 10, 5, 10)
    }
    val pnTrainR5 = new BoxPanel(Orientation.Horizontal) {
      contents += lbTrainR5
      contents += tfTrainPercent
      border = Swing.EmptyBorder(10, 0, 0, 0)
    }
    ////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    val pnTrain = new BoxPanel(Orientation.Vertical) {
      contents += pnTrainR1
      contents += pnTrainR2
      contents += pnTrainR3
      contents += pnTrainR4
      contents += pnTrainR5
      border = Swing.EmptyBorder(15, 10, 25, 15)
    }
    val pnTrainBoder = new BoxPanel(Orientation.Vertical) {
      contents += pnTrain
      border = Swing.LineBorder(Color.BLUE, 1)
    }
    val pnTrainGroup = new BoxPanel(Orientation.Vertical) {
      contents += pnTrainBoder
      border = Swing.EmptyBorder(15, 10, 15, 15)
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Test ~~~~~~~~~~~
    val lbTest = new Label {
      text = "CÁC THÔNG SỐ KIỂM THỬ"
    }
    val pnTestR1 = new BorderPanel {
      layout(lbTest) = BorderPanel.Position.West
    }
    val lbTestInput = new Label {
      text = "Đường dẫn tập tin văn bản kiểm thử (*.txt)           "
    }
    val tfTestLink = new TextField {
      columns = 1
      text = "/home/duytri/Desktop/test.txt"
      border = Swing.EmptyBorder(5, 10, 5, 10)
      enabled = false
    }
    val pnTestInput = new BoxPanel(Orientation.Horizontal) {
      contents += lbTestInput
      contents += tfTestLink
      border = Swing.EmptyBorder(10, 0, 0, 0)
    }
    val pnTest = new BoxPanel(Orientation.Vertical) {
      contents += pnTestR1
      contents += pnTestInput
      border = Swing.EmptyBorder(15, 10, 25, 15)
    }
    val pnTestBoder = new BoxPanel(Orientation.Vertical) {
      contents += pnTest
      border = Swing.LineBorder(Color.BLUE, 1)
    }
    val pnTestGroup = new BoxPanel(Orientation.Vertical) {
      contents += pnTestBoder
      border = Swing.EmptyBorder(0, 10, 25, 15)
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Center ~~~~~~~~~~~
    val pnCenter = new BoxPanel(Orientation.Vertical) {
      contents += pnTrainGroup
      contents += pnTestGroup
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Button ~~~~~~~~~~~
    val lbStatus = new Label {
      text = "Sẵn sàng..."
      font = new Font("Segoe UI Light", java.awt.Font.ITALIC, 14)
    }
    val btnTrain = new Button {
      text = "Huấn luyện"
      enabled = true
    }
    val btnTest = new Button {
      text = "Kiểm thử"
      enabled = false
    }
    val btnClose = new Button {
      text = "Đóng"
      enabled = true
    }
    val pnButtonRight = new BoxPanel(Orientation.Horizontal) {
      contents += btnTrain
      contents += new Label("   ")
      contents += btnTest
      contents += new Label("   ")
      contents += btnClose
      contents += new Label("        ")
    }
    val pnButton = new BorderPanel {
      layout(lbStatus) = BorderPanel.Position.West
      layout(pnButtonRight) = BorderPanel.Position.East
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Main ~~~~~~~~~~~
    contents = new BorderPanel {
      layout(pnTitle) = BorderPanel.Position.North
      layout(pnCenter) = BorderPanel.Position.Center
      layout(pnButton) = BorderPanel.Position.South
      border = Swing.EmptyBorder(15, 15, 17, 10)
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Spark ~~~~~~~~~~~
    val conf = new SparkConf().setAppName("ISLab.Petunia")
    val sc = new SparkContext(conf)
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Events ~~~~~~~~~~~
    var arrStopwords = new ArrayBuffer[String]
    var arrAttribute = new ArrayBuffer[String]
    var wordSet0 = new ArrayBuffer[Map[String, Int]]
    var wordSet1 = new ArrayBuffer[Map[String, Int]]
    listenTo(btnClose, btnTrain, btnTest)
    reactions += {
      case ButtonClicked(`btnClose`) =>
        closeOperation()
      case ButtonClicked(`btnTrain`) => {
        lbStatus.text = "Bắt đầu huấn luyện..."
        val bcTrainDir = sc.broadcast(tfTrainDir.text)
        val bcLowerBound = sc.broadcast(tfLowerBound.text.toDouble)
        val bcUpperBound = sc.broadcast(tfUpperBound.text.toDouble)
        val bcModelSaveDir = sc.broadcast(tfModelDir.text)
        val bcTrainRate = sc.broadcast(tfTrainPercent.text.toDouble)

        //lbStatus.text = "Đã phân tán các thông số đầu vào"

        //~~~~~~~~~~Get all data directories~~~~~~~~~~
        val inputDirPath = tfTrainDir.text + "input"
        val input = (inputDirPath + File.separator + "0", inputDirPath + File.separator + "1")
        println(input._1+"\n"+input._2)
        val bcInput1 = sc.broadcast(PUtils.getListOfSubFiles((new File(input._1))))
        val bcInput2 = sc.broadcast(PUtils.getListOfSubFiles((new File(input._2))))

        //~~~~~~~~~~Get all input files~~~~~~~~~~
        val listFiles0 = sc.parallelize(bcInput1.value)
        val listFiles1 = sc.parallelize(bcInput2.value)

        var wordSetByFile0: RDD[Map[String, Int]] = sc.emptyRDD[Map[String, Int]]
        //Foreach text file
        wordSetByFile0 = wordSetByFile0.union(listFiles0.map { fileDir =>
          PUtils.statWords(fileDir)
        })

        var wordSetByFile1: RDD[Map[String, Int]] = sc.emptyRDD[Map[String, Int]]
        //Foreach text file
        wordSetByFile1 = wordSetByFile1.union(listFiles1.map { fileDir =>
          PUtils.statWords(fileDir)
        })

        //lbStatus.text = "Đã thu thập và phân tán dữ liệu! Tính TF*IDF..."

        //~~~~~~~~~~Calculate TFIDF~~~~~~~~~~
        var tfidfWordSet0: RDD[Map[String, Double]] = sc.emptyRDD[Map[String, Double]] // Map[word, TF*IDF-value]
        wordSet0.appendAll(wordSetByFile0.collect)
        wordSet1.appendAll(wordSetByFile1.collect)
        val bcWordSet0 = sc.broadcast(wordSet0)
        val bcWordSet1 = sc.broadcast(wordSet1)
        tfidfWordSet0 = tfidfWordSet0.union(wordSetByFile0.map(oneFile => {
          PUtils.statTFIDF(oneFile, bcWordSet0.value)
        }))

        var tfidfWordSet1: RDD[Map[String, Double]] = sc.emptyRDD[Map[String, Double]] // Map[word, TF*IDF-value]
        tfidfWordSet1 = tfidfWordSet1.union(wordSetByFile1.map(oneFile => {
          PUtils.statTFIDF(oneFile, bcWordSet1.value)
        }))
        tfidfWordSet0.cache
        tfidfWordSet1.cache
        //lbStatus.text = "Đã tính xong TF*IDF cho tập dữ liệu! Loại bỏ stopwords..."

        //~~~~~~~~~~Remove stopwords~~~~~~~~~~
        //// Load stopwords from file
        val stopwordFilePath = bcTrainDir.value + "libs/vietnamese-stopwords.txt"
        val swSource = Source.fromFile(stopwordFilePath)
        swSource.getLines.foreach { x => arrStopwords.append(x) }
        swSource.close
        val bcStopwords = sc.broadcast(arrStopwords)
        //// Foreach document, remove stopwords
        tfidfWordSet0.foreach(oneFile => oneFile --= bcStopwords.value)
        tfidfWordSet1.foreach(oneFile => oneFile --= bcStopwords.value)

        //lbStatus.text = "Đã loại bỏ stopwords! Tiếp tục xử lý tập từ..."

        //~~~~~~~~~~Normalize by TFIDF~~~~~~~~~~
        val lowerUpperBound = (bcLowerBound.value, bcUpperBound.value)
        println("Argument 0 (lower bound): " + lowerUpperBound._1 + " - Argument 1 (upper bound): " + lowerUpperBound._2)
        var attrWords: RDD[String] = sc.emptyRDD[String]
        attrWords = attrWords.union(tfidfWordSet0.flatMap(oneFile => {
          oneFile.filter(x => x._2 > lowerUpperBound._1 && x._2 < lowerUpperBound._2).keySet
        }))
        attrWords = attrWords.union(tfidfWordSet1.flatMap(oneFile => {
          oneFile.filter(x => x._2 > lowerUpperBound._1 && x._2 < lowerUpperBound._2).keySet
        }))

        //lbStatus.text = "Tạo vector..."

        //~~~~~~~~~~Create vector~~~~~~~~~~
        var vectorWords: RDD[LabeledPoint] = sc.emptyRDD[LabeledPoint]
        arrAttribute.appendAll(attrWords.collect)
        val bcAttrWords = sc.broadcast(arrAttribute)
        vectorWords = vectorWords.union(tfidfWordSet0.map(oneFile => {
          var vector = new ArrayBuffer[Double]
          bcAttrWords.value.foreach { word =>
            {
              if (oneFile.contains(word)) {
                vector.append(oneFile.get(word).get)
              } else vector.append(0d)
            }
          }
          LabeledPoint(0d, Vectors.dense(vector.toArray))
        }))
        vectorWords = vectorWords.union(tfidfWordSet1.map(oneFile => {
          var vector = new ArrayBuffer[Double]
          bcAttrWords.value.foreach { word =>
            {
              if (oneFile.contains(word)) {
                vector.append(oneFile.get(word).get)
              } else vector.append(0d)
            }
          }
          LabeledPoint(1d, Vectors.dense(vector.toArray))
        }))
        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        //lbStatus.text = "Đang huấn luyện..."
        val splits = vectorWords.randomSplit(Array(0.7, 0.3), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1)

        val numIterations = 100
        val model = SVMWithSGD.train(training, numIterations)

        // Clear the default threshold.
        model.clearThreshold()
        model.setThreshold(0.5)

        //Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
          val score = model.predict(point.features)
          (score, point.label)
        }

        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        //val auPR = metrics.areaUnderPR

        val precision = metrics.precisionByThreshold.collect.toMap[Double, Double]
        val recall = metrics.recallByThreshold.collect.toMap[Double, Double]
        val fMeasure = metrics.fMeasureByThreshold.collect.toMap[Double, Double]

        //lbStatus.text = "Lưu mô hình"
        model.save(sc, bcModelSaveDir.value) //save in "myDir+data" may cause error

        //lbStatus.text = "Area under PR-Curve = " + auPR

        tfTestLink.enabled = true
        btnTest.enabled = true
        lbStatus.text = "Huấn luyện hoàn thành! Sẵn sàng để kiểm thử."
        Dialog.showMessage(contents.head,
          "Quá trình huấn luyện hoàn tất!\nKết quả:\n - Precision: " + precision.get(0.5).get + "\n - Recall: " + recall.get(0.5).get + "\n - F-measure: " + fMeasure.get(0.5).get,
          messageType = Dialog.Message.Info,
          title = title)
      }
      case ButtonClicked(`btnTest`) =>
        val testModel = SVMModel.load(sc, tfModelDir.text)
        testModel.clearThreshold()
        testModel.setThreshold(0.5)

        lbStatus.text = "Kiểm thử..."
        val testWords = PUtils.statWords(tfTestLink.text) //Load word set
        var tfidfTest = Map[String, Double]()
        //Calculate TFIDF
        tfidfTest ++= testWords.filter(wordSet0.contains).map(oneWord => {
          val tf = oneWord._2 / testWords.foldLeft(0d)(_ + _._2)
          val idf = Math.log10(wordSet0.size / wordSet0.filter(x => { x.contains(oneWord._1) }).length)
          oneWord._1 -> tf * idf
        })
        tfidfTest ++= testWords.filter(wordSet1.contains).map(oneWord => {
          val tf = oneWord._2 / testWords.foldLeft(0d)(_ + _._2)
          val idf = Math.log10(wordSet1.size / wordSet1.filter(x => { x.contains(oneWord._1) }).length)
          oneWord._1 -> tf * idf
        })
        //Remove stopwords
        tfidfTest --= arrStopwords
        //Create vector
        var testVector = new ArrayBuffer[Double]
        arrAttribute.foreach { word =>
          {
            if (tfidfTest.contains(word)) {
              testVector.append(tfidfTest.get(word).get)
            } else testVector.append(0d)
          }
        }
        //Test
        lbStatus.text = "Kiểm thử hoàn tất!"
        Dialog.showMessage(contents.head,
          "Quá trình kiểm thử  hoàn tất!\nKết quả: " + testModel.predict(Vectors.dense(testVector.toArray)),
          messageType = Dialog.Message.Info,
          title = title)
    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  }
}