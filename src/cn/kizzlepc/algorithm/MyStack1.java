package cn.kizzlepc.algorithm;

import java.util.Stack;

public class MyStack1 {
	private Stack<Integer> stackData;
	private Stack<Integer> stackMin;
	
	public MyStack1() {
		this.stackData = new Stack<Integer>();
		this.stackMin = new Stack<Integer>();
	}
	
	public void push(int newNum) {
		if(this.stackMin.isEmpty()) {
			this.stackMin.push(newNum);
		}else if(this.getMin() >= newNum) {
			this.stackMin.push(newNum);
		}
		this.stackData.push(newNum);
	}
	
	public int pop() {
		if(this.stackData.isEmpty()) {
			throw new RuntimeException("This stackData is empty.");
		}
		
		int value = this.stackData.pop();
		if(value == this.getMin()) {
			this.stackMin.pop();
		}
		return value;
	}
	public int getMin() {
		if(this.stackMin.isEmpty()) {
			throw new RuntimeException("This stackMin is empy.");
		}
		return this.stackMin.peek();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MyStack1 ms = new MyStack1();
		ms.push(5);
		ms.push(3);
		ms.push(1);
		ms.push(2);
		ms.push(4);
		System.out.println(ms.pop());
		System.out.println(ms.pop());
		System.out.println(ms.pop());
		System.out.println(ms.pop());
		System.out.println(ms.getMin());
	}

}
