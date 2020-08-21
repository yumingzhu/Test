package Apr;

import java.util.Date;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/22 14:11
 */
public class Block {

	public String hash;
	public String previousHash;
	private String data; //our data will be a simple message.
	private long timeStamp; //as number of milliseconds since 1/1/1970.
	//工作量证明
    private int nonce;

	//Block Constructor.
	public Block(String data, String previousHash) {
		this.data = data;
		this.previousHash = previousHash;
		this.timeStamp = new Date().getTime();
		this.hash = calculateHash(); //Making sure we do this after we set the other values.

	}

	public String calculateHash() {
		//根据 上一个区块 的hash   ，  当前的 timaStamp     还有 data   进行计算当前的  hash
		String calculatedhash = StringUtil
				.applySha256(previousHash + Long.toString(timeStamp) + Integer.toString(nonce) + data);
		return calculatedhash;
	}

	public void mineBlock(int diffculty) {
	    //Create a string with difficulty * "0"    00000
		String targer = new String(new char[diffculty]).replace("\0", "0");
		//判断 前 0-diffculty  是否等于 targer  如果不等于让nonce  进行累加  直到 hash 前几位 等于  diffculty
		while (!hash.substring(0, diffculty).equals(targer)) {
			nonce++;
			hash = calculateHash();
		}
		System.out.println("Block  Mined!!!:  " + hash);
	}

}
