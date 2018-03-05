package cn.kizzlepc.hadoop.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
id		date		pid		amount
1001	20150710	P0001	2

pid		pname		category_id		price
P0001	小米5			1000			2
 */
public class InfoBean implements Writable {
	private int id;
	private String dateString;
	private String pid;
	private int amount;
	private String pname;
	private int category_id;
	private long price;
	private String flag;
	
	public InfoBean() {	}
	
	public void set(int id, String dateString, String pid, int amount, String pname, int category_id, long price, String flag) {
		this.id = id;
		this.dateString = dateString;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.category_id = category_id;
		this.price = price;
		this.flag = flag;
	}

	@Override
	public String toString() {
		return id + "," + dateString + "," + pid + "," + amount + "," + pname + "," + category_id + "," + price;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDateString() {
		return dateString;
	}

	public void setDateString(String dateString) {
		this.dateString = dateString;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public int getCategory_id() {
		return category_id;
	}

	public void setCategory_id(int category_id) {
		this.category_id = category_id;
	}

	public long getPrice() {
		return price;
	}

	public void setPrice(long price) {
		this.price = price;
	}
	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		dateString = in.readUTF();
		pid = in.readUTF();
		amount = in.readInt();
		pname = in.readUTF();
		category_id = in.readInt();
		price = in.readLong();
		flag = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(dateString);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeInt(category_id);
		out.writeLong(price);
		out.writeUTF(flag);
	}

}
