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

TBD

### Integration based on Spray and Apache Kafka

TBD
