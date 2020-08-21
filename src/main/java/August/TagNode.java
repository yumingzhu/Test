package August;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/8/23 17:57
 */
public class TagNode {
	/**
	 * 描述当前两个节点的关系
	 */
	private String opeart;
	/**
	 * 第一个节点
	 */
	private String onetagId;
	/***
	 * 第二个节点
	 */
	private String twotagId;
	/***
	 *父亲节点
	 */
	private String parentNumber;
    /***
     * 当前节点标识
     */
	private  String number;

	/***
	 * 当前节点为第几层
	 */
	private Integer Level;

	public TagNode(String number,String opeart, String onetagId, String twotagId, String parentNumber, Integer level) {
		this.opeart = opeart;
		this.onetagId = onetagId;
		this.twotagId = twotagId;
		this.parentNumber = parentNumber;
		this.number=number;
		Level = level;
	}

	public TagNode(String number,String onetagId, String parentNumber, Integer level) {
		this.onetagId = onetagId;
		this.parentNumber = parentNumber;
        this.number=number;
        this.Level = level;
	}

	public String getOpeart() {
		return opeart;
	}

	public void setOpeart(String opeart) {
		this.opeart = opeart;
	}

	public String getOnetagId() {
		return onetagId;
	}

	public void setOnetagId(String onetagId) {
		this.onetagId = onetagId;
	}

	public String getTwotagId() {
		return twotagId;
	}

	public void setTwotagId(String twotagId) {
		this.twotagId = twotagId;
	}

	public Integer getLevel() {
		return Level;
	}

	public void setLevel(Integer level) {
		Level = level;
	}

    public String getParentNumber() {
        return parentNumber;
    }

    public void setParentNumber(String parentNumber) {
        this.parentNumber = parentNumber;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
}
