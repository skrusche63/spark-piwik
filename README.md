![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Integration of Piwik Analytics with Apache Spark 

Piwik is a widely used web analytics platform, and, it is an appropriate starting point for market basket analysis, user behavior analytics and more.

From [piwik.org](http://piwik.org/)
> Piwik is the leading open source web analytics platform that gives you valuable insights into your website’s visitors, your marketing campaigns and much more, so you can optimize your strategy and online experience of your visitors.

Integrating Piwik Analytics with Apache Kafka, Spark and other technologies from the Apache eco system enables to evaluate customer engagement data from Piwik with Association Rule & Frequent Sequence Mining, Context-Aware Recommendations, Markov Models and more to gain insights into customer engagement data far beyond traditional web analytics.

### Integration based on MySQL

The few lines of Scale code below show how to access customer engagement data persisted in Piwik's MySQL database:
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

```

#### Purchase Horizon

TBD

#### Relations between Ecommerce Items 

Association rule mining is a wide-spread method to discover interesting relations between items in large-scale databases. These relations 
are specified as so called *association rules*. A very popular application area for association rules is the detection of regularities between 
products in large-scale customer engagement data recorded by ecommerce websites or point-of-sale systems in supermarkets.

For example, the rule [onions, potatoes] -> [burger] indicates that if a customer buys onions and potatoes together, he or she is likely to also buy 
hamburger meat. Such information can be used as the basis for decisions about marketing activities such as, e.g., promotional pricing or product placements. 

In this project, we load customer engagement data from the `piwik_log_conversion_item` table into Apache Spark and transforms these data into an 
appropriate transaction format. To this end, all ecommerce items that refer to the same ecommerce order are aggregated into single line.

The output of this transformation has the following format:
```
idsite|idvisitor|idorder|timestamp|item item item ...
-----------------------------------------------------

1|b65ce95de5c8e7ea|A10000124|1407986582000|2 5 6 8 
1|b65ce95de5c8e7ea|A10000123|1407931845000|4 9
1|b65ce95de5c8e7ea|A10000125|1407986689000|3 5 7
...

```

The transformation is done by the following lines of Scala code:
```
  def fromLogConversion(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[String] = {

    val fields = LOG_FIELDS
    /*
     * Access to the log_conversion table is restricted to a time window,
     * specified by a start and end date of format yyyy-mm-dd
     */
    val query = sql_logConversion.replace("$1",startdate).replace("$2",enddate)   
    val rows = MySQLConnector.readTable(sc,url,database,user,password,idsite,query,fields)  
  
    /*
     * Restrict to conversion that refer to ecommerce orders (idgoal = 0)
     */
    rows.filter(row => isOrder(row)).map(row => {
      
      val idsite  = row("idsite").asInstanceOf[Long]
      /*
       * Convert 'idvisitor' into a HEX String representation
       */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)
      /*
       * Convert server_time into universal timestamp
       */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()
      
      val idorder = row("idorder").asInstanceOf[String]      
      val items = row("items").asInstanceOf[Int]
      
      /*
       * For further analysis it is actually sufficient to
       * focus on revenue_subtotal and revenue_discount
       */
      val revenue_subtotal = row("revenue_subtotal").asInstanceOf[Float]
      val revenue_discount = row("revenue_discount").asInstanceOf[Float]

      "" + idsite + "|" + user + "|" + idorder + "|" + timestamp + "|" + revenue_subtotal + "|" + revenue_discount
      
    })
    
  }
```

TBD

### Integration based on Spray and Apache Kafka

TBD
