package o2otrain;


class mycount{
	String merchant_id;
	int time;
	mycount(String merchant_id,int time) {
		// TODO Auto-generated constructor stub
		this.merchant_id=merchant_id;
		this.time=time;
	}
	void addone(){
		time++;
	}
}


class couponcount{
	String merchant_id;
	String discount_rate;
	int time;
	couponcount(String merchant_id,String discount_rate,int time) {
		// TODO Auto-generated constructor stub
		this.discount_rate=discount_rate;
		this.merchant_id=merchant_id;
		this.time=time;
	}
	void addone(){
		time++;
	}
}