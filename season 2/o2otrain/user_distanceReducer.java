package o2otrain;


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

public class user_distanceReducer  extends ReducerBase{
	  Record result;
	  @Override
	  public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	  }
	@Override
	  public void reduce(Record key, Iterator<Record> values, TaskContext context)
		throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); 
		long buy_cnt=0;
		long received_cnt=0;
		long received_buy_cnt=0;
		long []shangzhongxia_xun_lq={0,0,0};
		long []shangzhongxia_xun_xf={0,0,0};
		long []shangzhongxia_xun_lq_and_xf={0,0,0};
		long holiday_lq=0;
		long workday_lq=0;
		long holiday_xf=0;
		long workday_xf=0;
		List<Integer> avg_day=new ArrayList<Integer> ();
		List<mycount> merchant_list=new ArrayList<mycount>();//去商店的次数
		List<couponcount> coupon_list=new ArrayList<couponcount>();//去领取的次数
//		List<Double> avg_lq_dis=new ArrayList<Double>();
		
		
		////////////////
		
		List<Double> avg_lq_discount_rate_float=new ArrayList<Double> ();//new add
		List<Double> avg_lq_discount_first_float=new ArrayList<Double> ();//new add
		
		List<Double> avg_xf_discount_rate_float=new ArrayList<Double> ();//new add
		List<Double> avg_xf_discount_first_float=new ArrayList<Double> ();//new add
		
		
		
		
		int in15day=0;
		while(values.hasNext()){
			Record val=values.next();
			Date mytime_received=new Date();
			Date mytime_buy=new Date();
			
			if (val.getString("date_received")!=null ){
				received_cnt++;
				//-----------------------------------------
				try {
					mytime_received=sdf.parse(val.getString("date_received"));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (val.getString("date_received").substring(6, 8).compareTo("11")<0)
					shangzhongxia_xun_lq[0]++;
				else if (val.getString("date_received").substring(6, 8).compareTo("20")>0)
					shangzhongxia_xun_lq[2]++;
				else
					shangzhongxia_xun_lq[1]++;
				
				if (getWeek(mytime_received)==0 || getWeek(mytime_received)==6)
					holiday_lq++;
				else
					workday_lq++;
				
				avg_lq_discount_rate_float.add(val.getDouble("discount_rate_float"));
				avg_lq_discount_first_float.add(val.getDouble("discount_first_float"));
				if (is_exists(coupon_list,val.getString("merchant_id"), val.getString("discount_rate"))==-1){
					coupon_list.add(new couponcount(val.getString("merchant_id"),val.getString("discount_rate"),1));
				}
				else
				{
					coupon_list.get(is_exists(coupon_list,val.getString("merchant_id"), val.getString("discount_rate"))).addone();
				}
				
			}
			if (val.getString("date_pay")!=null ){
				try {
					mytime_buy=sdf.parse(val.getString("date_pay"));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				buy_cnt++;
				if (val.getString("date_pay").substring(6, 8).compareTo("11")<0)
					shangzhongxia_xun_xf[0]++;
				else if (val.getString("date_pay").substring(6, 8).compareTo("20")>0)
					shangzhongxia_xun_xf[2]++;
				else
					shangzhongxia_xun_xf[1]++;
				
				if (getWeek(mytime_buy)==0 || getWeek(mytime_buy)==6)
					holiday_xf++;
				else
					workday_xf++;
				
				if (is_exists(merchant_list, val.getString("merchant_id"))==-1){
					merchant_list.add(new mycount(val.getString("merchant_id"),1));
				}
				else
				{
					merchant_list.get(is_exists(merchant_list, val.getString("merchant_id"))).addone();
				}
				
			}
			if (val.getString("date_received")!=null && val.getString("date_pay")!=null){
				received_buy_cnt++;
				if (val.getString("date_received").substring(6, 8).compareTo("11")<0)
					shangzhongxia_xun_lq_and_xf[0]++;
				else if (val.getString("date_received").substring(6, 8).compareTo("20")>0)
					shangzhongxia_xun_lq_and_xf[2]++;
				else
					shangzhongxia_xun_lq_and_xf[1]++;
				avg_day.add(getBetween(mytime_buy, mytime_received));
				if (getBetween(mytime_buy, mytime_received)<=15)
					in15day++;//new add
				avg_xf_discount_rate_float.add(val.getDouble("discount_rate_float"));
				avg_xf_discount_first_float.add(val.getDouble("discount_first_float"));
			}
			
			//---------------------------------

			//--------------------------------------------------
//			if (val.getString("distance")!=null)
//				avg_lq_dis.add(Double.valueOf(val.getString("distance")));
			//--------------------------------------------------

			//------------------------------------
			
			
		}
		result.setString("user_id", key.getString("user_id"));
		result.setString("distance", key.getString("distance"));

		result.setBigint("user_distance_all_lq_train",Long.valueOf(received_cnt));
		result.setBigint("user_distance_shang_xun_lq_train",Long.valueOf(shangzhongxia_xun_lq[0]));
		result.setBigint("user_distance_zhong_xun_lq_train", Long.valueOf(shangzhongxia_xun_lq[1]) );
		result.setBigint("user_distance_xia_xun_lq_train", Long.valueOf(shangzhongxia_xun_lq[2]));
		result.setBigint("user_distance_holiday_lq_train", Long.valueOf(holiday_lq));
		result.setBigint("user_distance_workday_lq_train", Long.valueOf(workday_lq));
		//----------
		result.setBigint("user_distance_all_xf_train",Long.valueOf(buy_cnt));
		result.setBigint("user_distance_shang_xun_xf_train",Long.valueOf(shangzhongxia_xun_xf[0]));
		result.setBigint("user_distance_zhong_xun_xf_train", Long.valueOf(shangzhongxia_xun_xf[1]));
		result.setBigint("user_distance_xia_xun_xf_train", Long.valueOf(shangzhongxia_xun_xf[2]));
		result.setBigint("user_distance_holiday_xf_train", Long.valueOf(holiday_xf));
		result.setBigint("user_distance_workday_xf_train", Long.valueOf(workday_xf));
		//--------------
		result.setBigint("user_distance_all_lq_and_xf_train",Long.valueOf(received_buy_cnt));
		result.setDouble("user_distance_avg_day_train", avg_di(avg_day));
		result.setDouble("user_distance_lq_xf_zhuanhua_train", (double)received_buy_cnt/(received_cnt+1));
		result.setBigint("user_distance_shang_xun_lq_xf_train",Long.valueOf(shangzhongxia_xun_lq_and_xf[0]));
		result.setBigint("user_distance_zhong_xun_lq_xf_train", Long.valueOf(shangzhongxia_xun_lq_and_xf[1]));
		result.setBigint("user_distance_xia_xun_lq_xf_train", Long.valueOf(shangzhongxia_xun_lq_and_xf[2]));
		//----------------------

		result.setBigint("user_distance_lq_diff_merchant_num_train",Long.valueOf( merchant_list.size()));
		result.setBigint("user_distance_lq_diff_coupon_num_train",Long.valueOf( coupon_list.size()));
		//----------------
//		result.setDouble("user_lq_avg_dis", avg_dis(avg_lq_dis));
		//--------------------------
		result.setBigint("user_distance_lq_max_merchant_train",Long.valueOf( get_max_list(merchant_list)));
		result.setBigint("user_distance_lq_max_coupon_train",Long.valueOf( get_max_list_coupon(coupon_list)));
		
		
		
		////////////new
		result.setDouble("user_distance_lq_avg_discount_rate_float_train", avg_dis(avg_lq_discount_rate_float));
		result.setDouble("user_distance_lq_avg_discount_first_float_train", avg_dis(avg_lq_discount_first_float));
		
		result.setDouble("user_distance_xf_avg_discount_rate_float_train", avg_dis(avg_xf_discount_rate_float));
		result.setDouble("user_distance_xf_avg_discount_first_float_train", avg_dis(avg_xf_discount_first_float));
		result.setBigint("user_distance_lq_in15day_train",Long.valueOf(in15day));
		result.setDouble("user_distance_lq_in15_bili_train", (double)in15day/(received_cnt+1));
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
     
     
     static int is_exists(List<mycount> list,String merchant_id){
    	 for (int i=0; i<list.size(); ++i){
    		 if (list.get(i).merchant_id.equals(merchant_id)==true)
    			 return i;
    	 }
    	 return -1;
     }
     
     static int is_exists(List<couponcount> list,String merchant_id,String discount_rate){
    	 for (int i=0; i<list.size(); ++i){
    		 if (list.get(i).merchant_id.equals(merchant_id)==true && list.get(i).discount_rate.equals(discount_rate)==true)
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
     static double avg_di(List<Integer> a){
    	 if (a.size()==0)
    		 return 0;
    	 double sum=0;
    	 for (int i=0; i<a.size(); ++i){
    		 sum+=a.get(i);
    	 }
    	 return sum/a.size();
     }
     
     static int get_max_list(List<mycount> a){
    	 int max=0;
    	 if (a.size()==0) return 0;
    	 for (int i=0; i<a.size(); ++i){
    		 if (max<a.get(i).time) max=a.get(i).time;
    	 }
    	 return max;
     }
     
     
     static int get_max_list_coupon(List<couponcount> a){
    	 int max=0;
    	 if (a.size()==0) return 0;
    	 for (int i=0; i<a.size(); ++i){
    		 if (max<a.get(i).time) max=a.get(i).time;
    	 }
    	 return max;
     }
     
     
     int getBetween(Date a,Date b){
    	 long intervalMilli = a.getTime() - b.getTime();
         return (int) (intervalMilli / (24 * 60 * 60 * 1000));
     }
}

