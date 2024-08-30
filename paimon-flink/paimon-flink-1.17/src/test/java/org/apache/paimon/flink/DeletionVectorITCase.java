package org.apache.paimon.flink;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.Test;

/** ITCase for deletion vector table. */
public class DeletionVectorITCase extends CatalogITCaseBase {

	@Test
	public void testDVTableWithUpgrade() throws Exception {
		sEnv.executeSql(
				"CREATE TABLE T ("
						+ "    order_number BIGINT,"
						+ "    order_name   VARCHAR(3),"
						+ "    price        DECIMAL(32,2),"
						+ "    buyer        ROW<first_name STRING, last_name STRING>,"
						+ "    order_time   TIMESTAMP(3),"
						+ "    dt           DATE,"
						+ "    primary key (order_number, order_name, dt) NOT ENFORCED"
						+ ") PARTITIONED BY(dt) "
						+ "WITH ("
						+ "    'bucket' = '8',"
						+ "    'target-file-size' = '500mb',"
						+ "    'manifest.target-file-size' = '32mb',"
						+ "    'sink.parallelism' = '4',"
						+ "    'snapshot.expire.execution-mode' = 'async',"
						+ "    'deletion-vectors.enabled' = 'true'"
						+ ")");

		sEnv.executeSql(
				"CREATE TEMPORARY TABLE orders_gen ("
						+ "    order_number BIGINT,"
						+ "    order_name   VARCHAR(3),"
						+ "    price        DECIMAL(32,2),"
						+ "    buyer        ROW<first_name STRING, last_name STRING>,"
						+ "    order_time   TIMESTAMP(3),"
						+ "    dt           DATE"
						+ ") WITH ("
						+ "    'connector' = 'datagen', "
						+ "    'rows-per-second' = '10000',"
						+ "    'fields.order_number.min' = '1',"
						+ "    'fields.order_number.max' = '100'"
						+ ");");
		sEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
		TableResult result = sEnv.executeSql("INSERT INTO T SELECT * FROM orders_gen");
		Thread.sleep(1200000);
		result.getJobClient().get().cancel();
	}
}