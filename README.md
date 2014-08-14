![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Integration of Piwik Analytics with Apache Spark 

In this project, we illustrate that Apache Spark not only is a fast and general engine for large-scale data processing, but also an appropriate means to integrate existing data source and make their data 
applicable by sophisticated machine learning, mining and prediction algorithms. As a specific data source, we selected [Piwik Analytics](http://piwik.org/), which is a widely used open source platform for 
web analytics, and, an appropriate starting point for market basket analysis, user behavior analytics and more.

From [piwik.org](http://piwik.org/)
> Piwik is the leading open source web analytics platform that gives you valuable insights into your website’s visitors, your marketing campaigns and much more, so you can optimize your strategy and online experience of your visitors.

Integrating Piwik Analytics with Apache Kafka, Spark and other technologies from the Apache eco system enables to evaluate customer engagement data from Piwik with Association Rule & Frequent Sequence Mining, Context-Aware Recommendations, Markov Models and more to gain insights into customer engagement data far beyond traditional web analytics.

![Apache Spark and Piwik Analytics](https://raw.githubusercontent.com/skrusche63/spark-piwik/master/images/Apache-Spark-and-Piwik.png)

### Integration based on MySQL

The few lines of Scale code below show how to access customer engagement data persisted in Piwik's MySQL database. The connector requires the respective database location, name and user credentials. Customer engagement data 
are retrieved by specifying the unique identifier `idsite` of a certain website supported by Piwik, and a specific query statement `query`.
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

1|b65ce95de5c8e7ea|A10000124|1407986582000|1 2 4 5 
1|b65ce95de5c8e7ea|A10000123|1407931845000|2 3 5
1|b65ce95de5c8e7ea|A10000125|1407986689000|1 2 4 5
...

```

The transformation is done by the following lines of Scala code:
```
def fromLogConversionItem(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[String] = {

  val fields = LOG_ITEM_FIELDS  

  val query = sql_logConversionItem.replace("$1",startdate).replace("$2",enddate)    
  val rows = MySQLConnector.readTable(sc,url,database,user,password,idsite,query,fields)  
    
  val items = rows.filter(row => (isDeleted(row) == false)).map(row => {
      
    val idsite  = row("idsite").asInstanceOf[Long]
    val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     

    val user = new java.math.BigInteger(1, idvisitor).toString(16)

    val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
    val timestamp = server_time.getTime()
      
    val idorder = row("idorder").asInstanceOf[String]      
    val idaction_sku = row("idaction_sku").asInstanceOf[Long]
    
    (idsite,user,idorder,idaction_sku,timestamp)
    
  })

  items.groupBy(_._3).map(valu => {

    val data = valu._2.toList.sortBy(_._5)      
    val output = ArrayBuffer.empty[String]
      
    val (idsite,user,idorder,idaction_sku,timestamp) = data.head
    output += idaction_sku.toString
      
    for (record <- data.tail) {
      output += record._4.toString
    }
      
    "" + idsite + "|" + user + "|" + idorder + "|" + timestamp + "|" + output.mkString(" ")
      
  })
    
}
```
Discovering the Top K Association Rules from the transactions above, does not require any reference to certain website (`idsite`), visitor (`idvisitor`) or order (`idorder`). It is sufficient to
specify all items of a transaction in a single line, and assign a unique line number (`lno`):

```
lno|item item item ...
----------------------

0,4 232 141 6
1,169 129 16
2,16 6 175 126
3,16 124 141 175
4,16 124 175
5,232 4 238
...
```

The code below describes the `RuleBuilder` class that is responsible for discovering the association rules between the ecommerce items extracted from Piwik' database.
```
class RuleBuilder {

  /**
   * input = ["idsite|user|idorder|timestamp|items"]
   */
  def buildTopKRules(sc:SparkContext,dataset:RDD[String],k:Int=10,minconf:Double=0.8):String = {
    
    /* Prepare dataset */
    val transactions = prepare(sc,dataset)
    
    /* Extract rules and convert into JSON */
    val rules = TopK.extractRDDRules(sc,transactions,k,minconf)
    TopK.rulesToJson(rules)
     
  }
  
  def prepare(sc:SparkContext,dataset:RDD[String]):RDD[(Int,Array[String])] = {

    /* Reduce dataset to items and repartition to single partition */
    val items = dataset.map(line => line.split("|")(4)).coalesce(1)
    
    val index = sc.parallelize(Range.Long(0, items.count, 1),items.partitions.size)
    val zip = items.zip(index) 
    
    zip.map(valu => {
      
      val (line,no) = valu
      (no.toInt, line.split(" "))
      
    })
   
  }

} 
```

The table describes the result of Top K Association Rules, where `k = 10`  and the confidence threshold is set to `minconf = 0.8`.

| antecedent  | consequent | support | confidence |
| ------------- | ------------- |------------- | ------------- |
| 4 232        | 141  | 35 | 0.90 |
| 169 238      | 110  | 41 | 0.84 |
| 6 232        | 124  | 39 | 0.83 |
| 129 175      | 141  | 35 | 0.83 |
| 124 132 175  | 141  | 35 | 0.83 | 
| 124 126      | 132  | 37 | 0.82 |
| 175 232      | 124  | 38 | 0.81 |
| 16 124 141   | 132  | 36 | 0.80 |
| 16 232       | 141  | 37 | 0.80 |
| 16 175       | 141  | 41 | 0.80 |


TBD

### Integration based on Spray and Apache Kafka

TBD
