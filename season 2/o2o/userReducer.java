package o2o;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer.TaskContext;


import com.aliyun.odps.mapred.ReducerBase;

public class userReducer  extends ReducerBase{
	  Record result;
	  @Override
	  public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	  }
	@Override
	  public void reduce(Record key, Iterator<Record> values, TaskContext context)
		throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); 
		int all_lq=0;
		int[] shangzhongxia_xun={0,0,0};
		int holiday=0;
		int workday=0;
		List<goto_merchant> merchant_list=new ArrayList<goto_merchant>();//去商店的次数
		List<goto_merchant> coupon_list=new ArrayList<goto_merchant>();//去领取的次数
		List<Double> avg_lq_dis=new ArrayList<Double>();
		while (values.hasNext()) {
			Record val= values.next();
			all_lq++;
			if (val.getString("date_received").substring(6, 8).compareTo("11")<0)
				shangzhongxia_xun[0]++;
			else if (val.getString("date_received").substring(6, 8).compareTo("20")>0)
				shangzhongxia_xun[2]++;
			else
				shangzhongxia_xun[1]++;
			
			Date mytime=new Date();
			try {
				mytime=sdf.parse(val.getString("date_received"));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (getWeek(mytime)==0 || getWeek(mytime)==6)
				holiday++;
			else
				workday++;
			//--------------------------------------------------
			if (is_exists(merchant_list, val.getString("merchant_id"))==-1){
				merchant_list.add(new goto_merchant(val.getString("merchant_id"),1));
			}
			else
			{
				merchant_list.get(is_exists(merchant_list, val.getString("merchant_id"))).addone();
			}
			//--------------------------------------------------
			if (val.getDouble("distance")!=null)
				avg_lq_dis.add(Double.valueOf(val.getDouble("distance")));
			//--------------------------------------------------
			if (is_exists(coupon_list, val.getString("coupon_id"))==-1){
				coupon_list.add(new goto_merchant(val.getString("coupon_id"),1));
			}
			else
			{
				coupon_list.get(is_exists(coupon_list, val.getString("coupon_id"))).addone();
			}
			//-----------------------------------------------------
		}
		int output=0;
		result.setString("user_id", key.getString("user_id"));

		result.setBigint("user_all_lq",Long.valueOf(all_lq));
		result.setBigint("user_shang_xun_lq",Long.valueOf(shangzhongxia_xun[0]));
		result.setBigint("user_zhong_xun_lq", Long.valueOf(shangzhongxia_xun[1]) );
		result.setBigint("user_xia_xun_lq", Long.valueOf(shangzhongxia_xun[2]));
		result.setBigint("user_holiday_lq",Long.valueOf(holiday) );
		result.setBigint("user_workday_lq", Long.valueOf(workday));
		result.setDouble("user_shang_xun_lq_bili", (double)shangzhongxia_xun[0]/all_lq);
		result.setDouble("user_zhong_xun_lq_bili", (double)shangzhongxia_xun[1]/all_lq);	
		result.setDouble("user_xia_xun_lq_bili", (double)shangzhongxia_xun[2]/all_lq);
		result.setDouble("user_holiday_lq_bili", (double)holiday/all_lq);
		result.setDouble("user_workday_lq_bili", (double)workday/all_lq);
		result.setBigint("user_lq_diff_merchant_num",Long.valueOf( merchant_list.size()));
		result.setBigint("user_lq_diff_coupon_num",Long.valueOf( coupon_list.size()));
		result.setDouble("user_lq_diff_merchant_bili",(double) merchant_list.size()/all_lq);
		result.setDouble("user_lq_diff_coupon_bili",(double) coupon_list.size()/all_lq);
		result.setDouble("user_lq_avg_dis", avg_dis(avg_lq_dis));
		result.setBigint("user_lq_max_merchant",Long.valueOf( get_max_list(merchant_list)));
		result.setDouble("user_lq_max_merchant_bili",(double) get_max_list(merchant_list)/all_lq);
		result.setBigint("user_lq_max_coupon",Long.valueOf( get_max_list(coupon_list)));
		result.setDouble("user_lq_max_coupon_bili",(double) get_max_list(coupon_list)/all_lq);
		context.write(result);
	}
     static int getWeek(Date date){  
        
        Calendar cal = Calendar.getInstance();  
        cal.setTime(date);  
        int week_index = cal.get(Calendar.DAY_OF_WEEK) - 1;  
        if(week_index<0){  
            week_index = 0;  
        }   
        return week_index;  
    }  
     
     
     static int is_exists(List<goto_merchant> list,String merchant_id){
    	 for (int i=0; i<list.size(); ++i){
    		 if (list.get(i).merchant_id.equals(merchant_id)==true)
    			 return i;
    	 }
    	 return -1;
     }
     
     static double avg_dis(List<Double> a){
    	 if (a.size()==0)
    		 return 0;
    	 double sum=0;
    	 for (int i=0; i<a.size(); ++i){
    		 sum+=a.get(i);
    	 }
    	 return sum/a.size();
     }
     
     static int get_max_list(List<goto_merchant> a){
    	 int max=0;
    	 if (a.size()==0) return 0;
    	 for (int i=0; i<a.size(); ++i){
    		 if (max<a.get(i).time) max=a.get(i).time;
    	 }
    	 return max;
     }
}

class goto_merchant{
	String merchant_id;
	int time;
	goto_merchant(String merchant_id,int time) {
		// TODO Auto-generated constructor stub
		this.merchant_id=merchant_id;
		this.time=time;
	}
	void addone(){
		time++;
	}
}