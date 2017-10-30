package o2o;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class userDriver {

	public static void main(String[] args) throws OdpsException {
		// TODO Auto-generated method stub
		JobConf job = new JobConf();
		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("merchant_id:string,coupon_id:string,discount_rate:string,date_received:string,time_split:bigint,discount_rate_float:double,discount_first_float:double,distance:double"));
		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName("o2o_in").build(),job);
		OutputUtils.addTable(TableInfo.builder().tableName("o2o_out").build(),job);
		// TODO: specify a mapper
		job.setMapperClass(userMapper.class);
		job.setReducerClass(userReducer.class);
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
