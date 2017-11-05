package mimir;

import java.io._
import java.util.Vector

import org.rogach.scallop._

import mimir.algebra._
import mimir.exec.result.Row
import mimir.sql._
import mimir.util.ExperimentalOptions
//import net.sf.jsqlparser.statement.provenance.ProvenanceStatement
import net.sf.jsqlparser.statement.select.Select
import py4j.GatewayServer
import mimir.exec.Compiler
import mimir.gprom.algebra.OperatorTranslation
import org.gprom.jdbc.jna.GProMWrapper
import mimir.ctables.Reason

/**
 * The primary interface to Mimir.  Responsible for:
 * - Parsing and processing command line arguments.
 * - Initializing internal state (Database())
 * - Providing a command-line prompt (if appropriate)
 * - Invoking MimirJSqlParser and dispatching the 
 *   resulting statements to Database()
 *
 * Database() handles all of the logical dispatching,
 * Mimir provides a friendly command-line user 
 * interface on top of Database()
 */
object MimirVizier {

  var conf: MimirConfig = null;
  var db: Database = null;
  var gp: GProMBackend = null
  var usePrompt = true;
  var pythonMimirCallListeners = Seq[PythonMimirCallInterface]()

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())
    
    if(true){
      // Set up the database connection(s)
      db = new Database(new JDBCBackend(conf.backend(), conf.dbname()))
      db.backend.open()
    }
    else {
      //Use GProM Backend
      gp = new GProMBackend(conf.backend(), conf.dbname(), 1)
      db = new Database(gp)    
      db.backend.open()
      gp.metadataLookupPlugin.db = db;
    }
    
    db.initializeDBForMimir();

    if(ExperimentalOptions.isEnabled("INLINE-VG")){
        db.backend.asInstanceOf[InlinableBackend].enableInlining(db)
      }
    
    OperatorTranslation.db = db
    
    runServerForViztrails()
    
    db.backend.close()
    if(!conf.quiet()) { println("\n\nDone.  Exiting."); }
  }
  
  def runServerForViztrails() : Unit = {
    val server = new GatewayServer(this, 33388)
     server.addListener(new py4j.GatewayServerListener(){
        def connectionError(connExept : java.lang.Exception) = {
          println("Python GatewayServer connectionError: " + connExept)
        }
  
        def connectionStarted(conn : py4j.Py4JServerConnection) = {
          println("Python GatewayServer connectionStarted: " + conn)
        }
        
        def connectionStopped(conn : py4j.Py4JServerConnection) = {
          println("Python GatewayServer connectionStopped: " + conn)
        }
        
        def serverError(except: java.lang.Exception) = {
          println("Python GatewayServer serverError")
        }
        
        def serverPostShutdown() = {
           println("Python GatewayServer serverPostShutdown")
        }
        
        def serverPreShutdown() = {
           println("Python GatewayServer serverPreShutdown")
        }
        
        def serverStarted() = {
           println("Python GatewayServer serverStarted")
        }
        
        def serverStopped() = {
           println("Python GatewayServer serverStopped")
        }
     })
     server.start()
     
     while(true){
       Thread.sleep(90000)
       if(pythonCallThread != null){
         //println("Python Call Thread Stack Trace: ---------v ")
         //pythonCallThread.getStackTrace.foreach(ste => println(ste.toString()))
       }
       pythonMimirCallListeners.foreach(listener => {
       
          //println(listener.callToPython("knock knock, jvm here"))
         })
     }
     
    
  }
  
  //-------------------------------------------------
  //Python package defs
  ///////////////////////////////////////////////
  var pythonCallThread : Thread = null
  def loadCSV(file : String) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      println("loadCSV: From Vistrails: [" + file + "]") ;
      val csvFile = new File(file)
      val tableName = (csvFile.getName().split("\\.")(0) + "_RAW").toUpperCase
      if(db.getAllTables().contains(tableName)){
        println("loadCSV: From Vistrails: Table Already Exists: " + tableName)
      }
      else{
        db.loadTable(csvFile)
      }
      tableName 
    }
    println(s"loadCSV Took: ${timeRes._2}")
    timeRes._1
  }
  
  def createLens(input : Any, params : Seq[String], _type : String, make_input_certain:Boolean, materialize:Boolean) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      println("createLens: From Vistrails: [" + input + "] [" + params.mkString(",") + "] [" + _type + "]"  ) ;
      val paramsStr = params.mkString(",")
      val lenseName = "LENS_" + _type + ((input.toString() + _type + paramsStr + make_input_certain + materialize).hashCode().toString().replace("-", "") )
      var query:String = null
      db.getView(lenseName) match {
        case None => {
          if(make_input_certain){
            val materializedInput = "MATERIALIZED_"+input
            query = s"CREATE LENS ${lenseName} AS SELECT * FROM ${materializedInput} WITH ${_type}(${paramsStr})"  
            if(db.getAllTables().contains(materializedInput)){
                println("createLens: From Vistrails: Materialized Input Already Exists: " + materializedInput)
            }
            else{  
              val inputQuery = s"SELECT * FROM ${input}"
              val oper = db.sql.convert(db.parse(inputQuery).head.asInstanceOf[Select])
              //val virtOper = db.compiler.virtualize(oper, db.compiler.standardOptimizations)
              db.selectInto(materializedInput, oper)//virtOper.query)
            }
          }
          else{
            val inputQuery = s"SELECT * FROM ${input}"
            query = s"CREATE LENS ${lenseName} AS $inputQuery WITH ${_type}(${paramsStr})"  
          }
          println("createLens: query: " + query)
          db.update(db.parse(query).head) 
        }
        case Some(_) => {
          println("createLens: From Vistrails: Lens already exists: " + lenseName)
        }
      }
      if(materialize){
        if(!db.views(lenseName).isMaterialized)
          db.update(db.parse(s"ALTER VIEW ${lenseName} MATERIALIZE").head)
      }
      lenseName
    }
    println(s"createLens ${_type} Took: ${timeRes._2}")
    timeRes._1
  }
  
  def createView(input : Any, query : String) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      println("createView: From Vistrails: [" + input + "] [" + query + "]"  ) ;
      val inputSubstitutionQuery = query.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString) 
      val viewName = "VIEW_" + ((input.toString() + query).hashCode().toString().replace("-", "") )
      db.getView(viewName) match {
        case None => {
          val viewQuery = s"CREATE VIEW $viewName AS $inputSubstitutionQuery"  
          println("createView: query: " + viewQuery)
          db.update(db.parse(viewQuery).head)
        }
        case Some(_) => {
          println("createView: From Vistrails: View already exists: " + viewName)
        }
      }
      viewName
    }
    println(s"createView Took: ${timeRes._2}")
    timeRes._1
  }
  
  def vistrailsQueryMimir(query : String, includeUncertainty:Boolean, includeReasons:Boolean) : PythonCSVContainer = {
    val timeRes = time {
      println("vistrailsQueryMimir: " + query)
      val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
      if(includeUncertainty && includeReasons)
        operCSVResultsDeterminismAndExplanation(oper)
      else if(includeUncertainty)
        operCSVResultsDeterminism(oper)
      else 
        operCSVResults(oper)
    }
    println(s"vistrailsQueryMimir Took: ${timeRes._2}")
    timeRes._1
  }
  
  def explainCell(query: String, col:Int, row:String) : Seq[mimir.ctables.Reason] = {
    val timeRes = time {
      try {
      println("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
      val oper = totallyOptimize(db.sql.convert(db.parse(query).head.asInstanceOf[Select]))
      //val compiledOper = db.compiler.compileInline(oper, db.compiler.standardOptimizations)._1
      val cols = oper.columnNames
      //println(s"explaining Cell: [${cols(col)}][$row]")
      //db.explainCell(oper, RowIdPrimitive(row.toString()), cols(col)).toString()
      val provFilteredOper = db.explainer.filterByProvenance(oper,RowIdPrimitive(row))
      val subsetReasons = db.explainer.explainSubset(
              provFilteredOper, 
              Seq(cols(col)).toSet, false, false)
      db.explainer.getFocusedReasons(subsetReasons)
      } catch {
          case t: Throwable => {
            t.printStackTrace() // TODO: handle error
            Seq[Reason]()
          }
        }
      
    }
    println(s"explainCell Took: ${timeRes._2}")
    timeRes._1
  }
  
  def explainRow(query: String, row:String) : Seq[mimir.ctables.Reason] = {
    val timeRes = time {
      println("explainRow: From Vistrails: [ "+ row +" ] [" + query + "]"  ) ;
     // val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
      //val cols = oper.schema.map(f => f._1)
      //db.explainRow(oper, RowIdPrimitive(row)).toString()
      val oper = totallyOptimize(db.sql.convert(db.parse(query).head.asInstanceOf[Select]))
      //val compiledOper = db.compiler.compileInline(oper, db.compiler.standardOptimizations)._1
      val cols = oper.columnNames
      db.explainer.getFocusedReasons(db.explainer.explainSubset(
              db.explainer.filterByProvenance(oper,RowIdPrimitive(row)), 
              Seq().toSet, true, false))
    }
    println(s"explainRow Took: ${timeRes._2}")
    timeRes._1.distinct
  }

  
  def repairReason(reasons: Seq[mimir.ctables.Reason], idx:Int) : mimir.ctables.Repair = {
    val timeRes = time {
      println("repairReason: From Vistrails: [" + idx + "] [ " + reasons(idx) + " ]" ) ;
      reasons(idx).repair
    }
    println(s"repairReason Took: ${timeRes._2}")
    timeRes._1
  }
  
  def feedback(reasons: Seq[mimir.ctables.Reason], idx:Int, ack: Boolean, repairStr: String) : Unit = {
    val timeRes = time {
      println("feedback: From Vistrails: [" + idx + "] [ " + reasons(idx) + " ] [ " + ack + " ] [ " +repairStr+" ]" ) ;
      val reason = reasons(idx) 
      val argString = 
          if(!reason.args.isEmpty){
            " (" + reason.args.mkString(",") + ")"
          } else { "" }
      if(ack)
        db.update(db.parse(s"FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.guess }").head)
      else 
        db.update(db.parse(s"FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ repairStr }").head)
    }
    println(s"feedback Took: ${timeRes._2}")
  }
  
  def registerPythonMimirCallListener(listener : PythonMimirCallInterface) = {
    println("registerPythonMimirCallListener: From Vistrails: ") ;
    pythonMimirCallListeners = pythonMimirCallListeners.union(Seq(listener))
  }
  
  def getAvailableLenses() : String = {
    db.lenses.lensTypes.keySet.toSeq.mkString(",")
  }
  
  
  def getTuple(oper: mimir.algebra.Operator) : Map[String,PrimitiveValue] = {
    db.query(oper)(results => {
      val cols = results.schema.map(f => f._1)
      val colsIndexes = results.schema.zipWithIndex.map( _._2)
      if(results.hasNext){
        val row = results.next()
        colsIndexes.map( (i) => {
           (cols(i), row(i)) 
         }).toMap
      }
      else
        Map[String,PrimitiveValue]()
    })
  }
  
  def operCSVResults(oper : mimir.algebra.Operator) : PythonCSVContainer =  {
    db.query(oper)(results => {
    val cols = results.schema.map(f => f._1)
    val colsIndexes = results.schema.zipWithIndex.map( _._2)
    val rows = new StringBuffer()
    val prov = new Vector[String]()
    while(results.hasNext){
      val row = results.next()
      rows.append(colsIndexes.map( (i) => {
         row(i).toString 
       }).mkString(", ")).append("\n")
       prov.add(row.provenance.asString)
    }
    val resCSV = cols.mkString(", ") + "\n" + rows.toString()
    new PythonCSVContainer(resCSV, Array[Array[Boolean]](), Array[Boolean](), Array[Array[String]](), prov.toArray[String](Array()))
    })
  }
  
 def operCSVResultsDeterminism(oper : mimir.algebra.Operator) : PythonCSVContainer =  {
     val results = new Vector[Row]()
     var cols : Seq[String] = null
     var colsIndexes : Seq[Int] = null
     
     db.query(oper)( resIter => {
         cols = resIter.schema.map(f => f._1)
         colsIndexes = resIter.schema.zipWithIndex.map( _._2)
         while(resIter.hasNext())
           results.add(resIter.next)
     })
     val resCSV = results.toArray[Row](Array()).seq.map(row => {
       val truples = colsIndexes.map( (i) => {
         (row(i).toString, row.isColDeterministic(i)) 
       }).unzip
       (truples._1.mkString(", "), truples._2.toArray, (row.isDeterministic(), row.provenance.asString))
     }).unzip3
     val rowDetAndProv = resCSV._3.unzip
     new PythonCSVContainer(resCSV._1.mkString(cols.mkString(", ") + "\n", "\n", ""), resCSV._2.toArray, rowDetAndProv._1.toArray, Array[Array[String]](), rowDetAndProv._2.toArray)
  }
 
 def operCSVResultsDeterminismAndExplanation(oper : mimir.algebra.Operator) : PythonCSVContainer =  {
     val results = new Vector[Row]()
     var cols : Seq[String] = null
     var colsIndexes : Seq[Int] = null
     
     db.query(oper)( resIter => {
         cols = resIter.schema.map(f => f._1)
         colsIndexes = resIter.schema.zipWithIndex.map( _._2)
         while(resIter.hasNext())
           results.add(resIter.next)
     })
     val resCSV = results.toArray[Row](Array()).seq.map(row => {
       val truples = colsIndexes.map( (i) => {
         (row(i).toString, row.isColDeterministic(i), if(!row.isColDeterministic(i))db.explainCell(oper, row.provenance, cols(i)).reasons.mkString(",")else"") 
       }).unzip3
       (truples._1.mkString(", "), (truples._2.toArray, row.isDeterministic(), row.provenance.asString), truples._3.toArray)
     }).unzip3
     val detListsAndProv = resCSV._2.unzip3
     new PythonCSVContainer(resCSV._1.mkString(cols.mkString(", ") + "\n", "\n", ""), detListsAndProv._1.toArray, detListsAndProv._2.toArray, resCSV._3.toArray, detListsAndProv._3.toArray)
  }
 
 def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }  
 
 def totallyOptimize(oper : mimir.algebra.Operator) : mimir.algebra.Operator = {
    val preOpt = oper.toString() 
    val postOptOper = db.compiler.optimize(oper)
    val postOpt = postOptOper.toString() 
    if(preOpt.equals(postOpt))
      postOptOper
    else
      totallyOptimize(postOptOper)
  }
 
}

//----------------------------------------------------------

trait PythonMimirCallInterface {
	def callToPython(callStr : String) : String
}

class PythonCSVContainer(val csvStr: String, val colsDet: Array[Array[Boolean]], val rowsDet: Array[Boolean], val celReasons:Array[Array[String]], val prov: Array[String]){}

