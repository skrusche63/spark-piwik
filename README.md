![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Integration of Piwik Analytics with Apache Spark 

Piwik is a widely used web analytics platform, and, it is an appropriate starting point for market basket analysis, user behavior analytics and more.

From [piwik.org](http://piwik.org/)
> Piwik is the leading open source web analytics platform that gives you valuable insights into your website’s visitors, your marketing campaigns and much more, so you can optimize your strategy and online experience of your visitors.

Integrating Piwik Analytics with Apache Kafka, Spark and other technologies from the Apache eco system enables to use Piwik with Association Rule & Frequent Sequence Mining, Context-Aware Recommendations, Markov Models and more to gain insights into customer engagement data far beyond traditional web analytics.

### Integration based on MySQL

The few lines of Scale code below show howto easily access Piwik data persisted in a MySQL database table:
```
object MySQLConnector {

  private val MYSQL_DRIVER   = "com.mysql.jdbc.Driver"
  private val NUM_PARTITIONS = 1
   
  def readTable(sc:SparkContext,url:String,database:String,user:String,password:String,idsite:Int,query:String,fields:List[String]):RDD[Map[String,Any]] = {
    
    val result = new JdbcRDD(sc,() => getConnection(url,database,user,password),
      query,idsite,idsite,NUM_PARTITIONS,
      (rs:ResultSet) => getRow(rs,fields)
    ).cache()

    result
    
  }
  
  /**
   * Convert database row into Map[String,Any]
   */
  private def getRow(rs:ResultSet,fields:List[String]):Map[String,Any] = {
    
    val metadata = rs.getMetaData()
    val numCols  = metadata.getColumnCount()
    
    val row = HashMap.empty[String,Any]
    (1 to numCols).foreach(i => {
      
      val k = metadata.getColumnName(i)
      val v = rs.getObject(i)
      
      if (fields.isEmpty) {
        row += k -> v
        
      } else {        
        if (fields.contains(k)) row += k -> v
        
      }
      
    })
   
    row.toMap
    
  }

```


### Integration based on Spray and Apache Kafka

