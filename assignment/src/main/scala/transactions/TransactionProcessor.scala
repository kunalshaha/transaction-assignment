package transactions

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object TransactionProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("spark://spark-master:7077")
     .appName("Transaction Processor").getOrCreate()
    /**
     * Read data from each directory
     *
     * /Users/kunalshaha/personal/docker-images/docker-spark-cluster/data/accounts/
     * /opt/spark-data/
     */
    val accountDf = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").option("multiline","true").load("/opt/spark-data/accounts/*")
    val creditDf = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").option("multiline","true").load("/opt/spark-data/cards/*")
    val savingsDf = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").option("multiline","true").load("/opt/spark-data/savings_accounts/*")

    /**
     * Filter update operations
     * as only transactions data is required
     */
    val filterdedAccountDF = accountDf.filter(col("op")  === 'u')
    /**
     *+----+----------+---+--------------------+-------------+
      |data|        id| op|                 set|           ts|
      +----+----------+---+--------------------+-------------+
      |null|a1globalid|  u|[Jakarta,, anthon...|1577894400000|
      |null|a1globalid|  u|          [,,,, sa1]|1577890800000|
      |null|a1globalid|  u|     [,,, 87654321,]|1577865600000|
      |null|a1globalid|  u|           [, c1,,,]|1577926800000|
      |null|a1globalid|  u|           [, c2,,,]|1579163400000|
      |null|a1globalid|  u|             [, ,,,]|1579078860000|
      +----+----------+---+--------------------+-------------+
     */
    val filteredUpdateOperCreditDF =creditDf.filter(col("op") === 'u')
    /**
     *   +----+----------+---+----------+-------------+
          |data|        id| op|       set|           ts|
          +----+----------+---+----------+-------------+
          |null|c2globalid|  u|  [37000,]|1579361400000|
          |null|c1globalid|  u|  [12000,]|1578313800000|
          |null|c1globalid|  u|  [19000,]|1578420000000|
          |null|c1globalid|  u|[, CLOSED]|1579078800000|
          |null|c1globalid|  u|[, ACTIVE]|1578159000000|
          |null|c2globalid|  u|[, ACTIVE]|1579298400000|
          |null|c1globalid|  u|      [0,]|1578654000000|
          +----+----------+---+----------+-------------+
     */
    val filteredUpdateOperSavingOper = savingsDf.filter(col("op") === 'u')
    /**
       * +----+-----------+---+--------+-------------+
        |data|         id| op|     set|           ts|
        +----+-----------+---+--------+-------------+
        |null|sa1globalid|  u| [, 3.0]|1578159060000|
        |null|sa1globalid|  u| [, 4.0]|1579298460000|
        |null|sa1globalid|  u| [, 1.5]|1579078860000|
        |null|sa1globalid|  u|[40000,]|1578648600000|
        |null|sa1globalid|  u|[21000,]|1578654000000|
        |null|sa1globalid|  u|[33000,]|1579505400000|
        |null|sa1globalid|  u|[15000,]|1577955600000|
        +----+-----------+---+--------+-------------+
      */

    val onlyLinkedAccounts = filterdedAccountDF.filter(col("set.savings_account_id").isNotNull
      or col("set.card_id").isNotNull)
    /**
     *  To fetch only linked Credit & Savings  from Accounts table
     * +----+----------+---+----------+-------------+
      |data|        id| op|       set|           ts|
      +----+----------+---+----------+-------------+
      |null|a1globalid|  u|[,,,, sa1]|1577890800000|
      |null|a1globalid|  u| [, c1,,,]|1577926800000|
      |null|a1globalid|  u| [, c2,,,]|1579163400000|
      |null|a1globalid|  u|   [, ,,,]|1579078860000|
      +----+----------+---+----------+-------------+
     */
    val activeSavingsAcc =  onlyLinkedAccounts.filter(col("set.savings_account_id").isNotNull)
    val activeCreditAcc =  onlyLinkedAccounts.filter(col("set.card_id").isNotNull)

    val savingsCreateOper = savingsDf.filter(col("op") === 'c')
    val savingAccountOpening = savingsCreateOper.filter(col("data.savings_account_id").isNotNull)
    /**
     * Fetch Only the account opening/active rec from Savings
     * +--------------------+-----------+---+----+-------------+
      |                data|         id| op| set|           ts|
      +--------------------+-----------+---+----+-------------+
      |[0, 1.5, sa1, ACT...|sa1globalid|  c|null|1577890800000|
      +--------------------+-----------+---+----+-------------+
     */
    val idForSavingToPullTransaction = activeSavingsAcc.alias("a").join(savingAccountOpening.alias("b")
      ,col("b.data.savings_account_id")
      === col("a.set.savings_account_id")).select(col("a.id")
      .as("accountId") ,col("b.id").as("id")  , col("b.ts").as("ac_ts"))
    /**
     * Join on the saving account number  on savings_account_id (sa1) field fetched from accounts table , and use the id to fetch resp
     * transaction rec from saving table
     * +----------+-----------+-------------+
      | accountId|         id|        ac_ts|
      +----------+-----------+-------------+
      |a1globalid|sa1globalid|1577890800000|
      +----------+-----------+-------------+
     */
    val savingTransactions =  filteredUpdateOperSavingOper.filter(col("set.balance").isNotNull)
      .join(idForSavingToPullTransaction,Seq("id")).orderBy("ts")
      .select(col("accountId"),col("id").as("bank_acc_id")
      ,col("set.balance").as("sv_trn_amt"),col("ts").as("transaction_time"))
      .withColumn("account_type",lit("saving_account"))
      .withColumn("friendly_date",from_unixtime(col("transaction_time")/1000,"yyyy-MM-dd HH:mm:ss"))
    /**
     * Fetch all the records for the resp bank_acc_id  and convert the timetsamp to friendly_date
     * +----------+-----------+----------+----------------+--------------+-------------------+
    | accountId|bank_acc_id|sv_trn_amt|transaction_time|  account_type|      friendly_date|
    +----------+-----------+----------+----------------+--------------+-------------------+
    |a1globalid|sa1globalid|     15000|   1577955600000|saving_account|2020-01-02 17:00:00|
    |a1globalid|sa1globalid|     40000|   1578648600000|saving_account|2020-01-10 17:30:00|
    |a1globalid|sa1globalid|     21000|   1578654000000|saving_account|2020-01-10 19:00:00|
    |a1globalid|sa1globalid|     33000|   1579505400000|saving_account|2020-01-20 15:30:00|
    +----------+-----------+----------+----------------+--------------+-------------------+
     */
    val window = Window.partitionBy("bank_acc_id").orderBy(col("transaction_time").asc)
    val lagColSvg = lag(col("sv_trn_amt"), 1).over(window)

    val finalSaving = savingTransactions.withColumn("lagColSvg", lagColSvg).orderBy(col("transaction_time"))
      .withColumn("diff_sv", when(col("lagColSvg").isNull,col("sv_trn_amt"))
        .otherwise( col("sv_trn_amt") - col("lagColSvg"))).
      select(col("accountId"),col("bank_acc_id"),col("transaction_time"),
        col("friendly_date"),col("diff_sv")).withColumn("diff_cr",lit("0"))
    /**
     * Apply Windowing funtion on the upstream result se and partion the same on bank_acc_id
     * use lag function to in order to calculate the difference and transactions occured , diff_sv shows
     * the amounts
     *
    +----------+-----------+----------+----------------+--------------+----------+-------------------+---------+-------+
    | accountId|bank_acc_id|sv_trn_amt|transaction_time|  account_type|freindly_date|lagColSvg|diff_sv|
    +----------+-----------+----------+----------------+--------------+----------+-------------------+---------+-------+
    |a1globalid|sa1globalid|     15000|   1577955600000|saving_account|2020-01-02 17:00:00| null|  15000|
    |a1globalid|sa1globalid|     40000|   1578648600000|saving_account|2020-01-10 17:30:00|15000|  25000|
    |a1globalid|sa1globalid|     21000|   1578654000000|saving_account|2020-01-10 19:00:00|40000| -19000|
    |a1globalid|sa1globalid|     33000|   1579505400000|saving_account|2020-01-20 15:30:00|21000|  12000|
    +----------+-----------+----------+----------------+--------------+----------+-------------------+---------+-------+

    +----------+-----------+----------------+-------------------+-------+-------+
    | accountId|bank_acc_id|transaction_time|      freindly_date|diff_sv|diff_cr|
    +----------+-----------+----------------+-------------------+-------+-------+
    |a1globalid|sa1globalid|   1577955600000|2020-01-02 17:00:00|  15000|      0|
    |a1globalid|sa1globalid|   1578648600000|2020-01-10 17:30:00|  25000|      0|
    |a1globalid|sa1globalid|   1578654000000|2020-01-10 19:00:00| -19000|      0|
    |a1globalid|sa1globalid|   1579505400000|2020-01-20 15:30:00|  12000|      0|
    +----------+-----------+----------------+-------------------+-------+-------+
     */


    val creditCreateRec = creditDf.filter(col("op") === 'c' && col("data.card_id").isNotNull)
    /**
     * Fetch the credit cards associated with accounts
     * +--------------------+----------+---+----+-------------+
      |                data|        id| op| set|           ts|
      +--------------------+----------+---+----+-------------+
      |[c1, 11112222, 0,...|c1globalid|  c|null|1577926800000|
      |[c2, 12123434, 0,...|c2globalid|  c|null|1579163400000|
      +--------------------+----------+---+----+-------------+
     */
    val idForCreditToPullTransaction = activeCreditAcc.alias("a")
      .join(creditCreateRec.alias("b"),col("b.data.card_id") === col("a.set.card_id"))
      .select(col("a.id").as("accountId") ,col("b.id").as("id")
        , col("b.ts").as("ac_ts"))
    /**
     * Join accounts table  & credit card table on card_id and get the dbid to fetch the respective transactions
     * from credit card table
     * +----------+----------+-------------+
    | accountId|        id|        ac_ts|
    +----------+----------+-------------+
    |a1globalid|c1globalid|1577926800000|
    |a1globalid|c2globalid|1579163400000|
    +----------+----------+-------------+
     */
    val creditTransactions =  filteredUpdateOperCreditDF.filter(col("set.credit_used").isNotNull)
      .join(idForCreditToPullTransaction,Seq("id")).orderBy("ts")
      .select(col("accountId"),col("id").as("bank_acc_id")
        ,col("set.credit_used").as("cr_trn_amt"),col("ts").as("transaction_time"))
      .withColumn("account_type",lit("credit_card"))
      .withColumn("friendly_date",from_unixtime(col("transaction_time")/1000,"yyyy-MM-dd HH:mm:ss"))



    val lagCol = lag(col("cr_trn_amt"), 1).over(window)
    val finalCreditVal =  creditTransactions.withColumn("lagCol", lagCol).orderBy(col("transaction_time"))
      .withColumn("diff", when(col("lagCol").isNull,col("cr_trn_amt"))
        .otherwise( col("cr_trn_amt") - col("lagCol"))).select(col("accountId"),col("bank_acc_id")
      ,col("transaction_time"),col("friendly_date"),col("diff")
        .as("diff_cr")).withColumn("diff_sv",lit("0"))

    /**
     * Apply Windowing funtion on the upstream result and partion the same on bank_acc_id
     * use lag function to in order to calculate the difference and transactions occured , diff shows
     * the amounts
     *
    +----------+-----------+----------+----------------+------------+----------+-------------------+------+------+
    | accountId|bank_acc_id|cr_trn_amt|transaction_time|account_type|sv_trn_amt|      freindly_date|lagCol|  diff|
    +----------+-----------+----------+----------------+------------+----------+-------------------+------+------+
    |a1globalid| c1globalid|     12000|   1578313800000| credit_card|         0|2020-01-06 20:30:00|  null| 12000|
    |a1globalid| c1globalid|     19000|   1578420000000| credit_card|         0|2020-01-08 02:00:00| 12000|  7000|
    |a1globalid| c1globalid|         0|   1578654000000| credit_card|         0|2020-01-10 19:00:00| 19000|-19000|
    |a1globalid| c2globalid|     37000|   1579361400000| credit_card|         0|2020-01-18 23:30:00|  null| 37000|
    +----------+-----------+----------+----------------+------------+----------+-------------------+------+------+

    +----------+-----------+----------+----------------+------------+-------------------+
    | accountId|bank_acc_id|cr_trn_amt|transaction_time|account_type|      friendly_date|
    +----------+-----------+----------+----------------+------------+-------------------+
    |a1globalid| c1globalid|     12000|   1578313800000| credit_card|2020-01-06 20:30:00|
    |a1globalid| c1globalid|     19000|   1578420000000| credit_card|2020-01-08 02:00:00|
    |a1globalid| c1globalid|         0|   1578654000000| credit_card|2020-01-10 19:00:00|
    |a1globalid| c2globalid|     37000|   1579361400000| credit_card|2020-01-18 23:30:00|
    +----------+-----------+----------+----------------+------------+-------------------+
     */
    val transactionList = finalSaving.select(col("accountId"),col("bank_acc_id"),col("transaction_time")
      ,col("friendly_date"),col("diff_cr"),col("diff_sv"))
      .union(finalCreditVal.select(col("accountId"),col("bank_acc_id")
        ,col("transaction_time"),col("friendly_date"),col("diff_cr")
        ,col("diff_sv"))).withColumnRenamed("diff_cr","card_used_diff")
      .withColumnRenamed("diff_sv","balance_diff").orderBy(col("transaction_time").asc)

    ///opt/spark-data/output/
    transactionList.coalesce(1).write
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode("overwrite").option("header", "true")
      .save("/opt/spark-data/output/");
    /**
     * union both the tables and order them by teansaction_time
     * +----------+-----------+----------------+-------------------+-------+-------+
    | accountId|bank_acc_id|transaction_time|      freindly_date|diff_cr|diff_sv|
    +----------+-----------+----------------+-------------------+-------+-------+
    |a1globalid|sa1globalid|   1577955600000|2020-01-02 17:00:00|      0|  15000|
    |a1globalid| c1globalid|   1578313800000|2020-01-06 20:30:00|  12000|      0|
    |a1globalid| c1globalid|   1578420000000|2020-01-08 02:00:00|   7000|      0|
    |a1globalid|sa1globalid|   1578648600000|2020-01-10 17:30:00|      0|  25000|
    |a1globalid| c1globalid|   1578654000000|2020-01-10 19:00:00| -19000|      0|
    |a1globalid|sa1globalid|   1578654000000|2020-01-10 19:00:00|      0| -19000|
    |a1globalid| c2globalid|   1579361400000|2020-01-18 23:30:00|  37000|      0|
    |a1globalid|sa1globalid|   1579505400000|2020-01-20 15:30:00|      0|  12000|
    +----------+-----------+----------------+-------------------+-------+-------+
     */
  }

}
