package o2otrain;


import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class user_couponMapper extends MapperBase {
	private Record key;
	private Record value;
	@Override
	public void setup(TaskContext context) throws IOException {
		key=context.createMapOutputKeyRecord();
		value=context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		key.setString("user_id", record.getString("user_id"));
		key.setString("merchant_id",record.getString("merchant_id"));
		value.setString("coupon_id",record.getString("coupon_id"));
		key.setString("discount_rate",record.getString("discount_rate"));
		value.setString("date_received",record.getString("date_received"));
		value.setString("date_pay",record.getString("date_pay"));
		value.setString("distance",record.getString("distance"));
		context.write(key, value);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
