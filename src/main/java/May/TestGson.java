package May;

import August.TagNode;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/21 11:36
 */
public class TestGson {
	public static void main(String[] args) throws IOException {
		Gson gson = new Gson();
		//		User user = new User("张三", 24, "zhangsan@ceshi.com");
		//		String jsonObject = gson.toJson(user); // {"name":"张三kidou","age":24}
		//
		//		User user1 = gson.fromJson(jsonObject, User.class);
		//		System.out.println(user1);
		//
		//		User user2 = gson.fromJson("{\"name\":\"张三\",\"age\":24,\"email_address\":\"zhangsan@ceshi.com\"}", User.class);
		//		System.out.println(user2);
		//		String jsonArray = "[\"Android\",\"Java\",\"PHP\"]";
		//		List<String> stringList = gson.fromJson(jsonArray, new TypeToken<List<String>>() {
		//		}.getType());
		//		System.out.println(stringList);
		//		User user3 = new User();
		//		JsonReader reader = new JsonReader(new StringReader(jsonObject));
		//		reader.beginObject();
		//		while (reader.hasNext()) {
		//			String s = reader.nextName();
		//			switch (s) {
		//			case "name":
		//				user3.name = reader.nextString();
		//				break;
		//			case "age":
		//				user3.age = reader.nextInt();
		//				break;
		//			case "emailAddress":
		//				user3.emailAddress = reader.nextString();
		//			}
		//		}
		//		reader.endObject();
		//        System.out.println(user3);

		String value = "{\"data\":{\"boolean\":\"&&\",\"split\":true,\"isLeaf\":false,\"children\":[{\"boolean\":\"&&\","
				+ "\"split\":true,\"isLeaf\":false,\"children\":[{\"boolean\":\"||\",\"split\":false,\"isLeaf\":false,"
				+ "\"children\":[{\"tagId\":\"A0010\",\"name\":\"20-25岁\",\"number\":56741103,\"surviveTime\":\"6\","
				+ "\"frequency\":\"1\",\"type\":3,\"isLeaf\":true,\"uid\":4,\"pid\":3},{\"tagId\":\"A0011\","
				+ "\"name\":\"26-30岁\",\"number\":123284652,\"surviveTime\":\"6\",\"frequency\":\"1\",\"type\":3,"
				+ "\"isLeaf\":true,\"uid\":5,\"pid\":3}],\"uid\":3,\"pid\":2},{\"boolean\":\"||\",\"split\":false,"
				+ "\"isLeaf\":false,\"children\":[{\"tagId\":\"B0066\",\"name\":\"有车\",\"number\":140693535,\"type\":1,"
				+ "\"isLeaf\":true,\"uid\":7,\"pid\":6}],\"pid\":2,\"uid\":6}],\"uid\":2,\"pid\":1},"
				+ "{\"boolean\":\"||\",\"split\":false,\"isLeaf\":false,\"children\":[{\"tagId\":\"B0053\","
				+ "\"name\":\"自驾出行\",\"number\":363552,\"type\":1,\"isLeaf\":true,\"uid\":9,\"pid\":8},"
				+ "{\"tagId\":\"B0054\",\"name\":\"租车出行\",\"number\":676054,\"type\":1,\"isLeaf\":true,\"uid\":10,"
				+ "\"pid\":8}],\"pid\":1,\"uid\":8}],\"uid\":1}}";
		// 需要把当前 层级结构转化为可以执行的链接
		InfoBean infoBean = gson.fromJson(value, InfoBean.class);
		Map<Integer, List<TagNode>> listMap = infoBean.parseNodedata();
		System.out.println("---");
	}

}

//需要找到一个可执行的关系链，

/***
 *
 */
class InfoBean {
	/**
	 * key 层级，    TagNode 为一个执行单元
	 */
	Map<Integer, List<TagNode>> listHashMap = new HashMap<Integer, List<TagNode>>();

	public InfoBean getInfoBean(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, InfoBean.class);
	}

	private Node data;

	public Node getData() {
		return data;
	}

	public void setData(Node data) {
		this.data = data;
	}

	public Map<Integer, List<TagNode>> parseNodedata() {
		data.parseNode(listHashMap, 1, 0 + "");
		return listHashMap;
	}
}

class Node {
	@SerializedName(value = "opeart", alternate = { "boolean" })
	public String opeart;
	public List<Node> children;
	public String tagId;

	public String getOpeart() {
		return opeart;
	}

	public void setOpeart(String opeart) {
		this.opeart = opeart;
	}

	public List<Node> getChildren() {
		return children;
	}

	public void setChildren(List<Node> children) {
		this.children = children;
	}

	public String getTagId() {
		return tagId;
	}

	public void setTagId(String tagId) {
		this.tagId = tagId;
	}

	/**
	 * level 为  当前的层级
	 * @param listHashMap
	 * @param level
	 */
	public void parseNode(Map<Integer, List<TagNode>> listHashMap, int level, String parentNode) {
		String opeart = this.getOpeart();
		TagNode tagNode = null;
		if (StringUtils.isNotBlank(opeart)) {
			List<Node> children = this.getChildren();
			if (children.size() == 2) {
				int accumulatorValue = Accumulator.getAccumulatorValue();
				Node oneNode = children.get(0);
				Node twoNode = children.get(1);

				if (oneNode.getOpeart() != null) {
					if (twoNode.getOpeart() != null) {
						tagNode = new TagNode(accumulatorValue + "", opeart, Accumulator.getAccumulatorValue() + "",
								Accumulator.getAccumulatorValue() + "", parentNode, level);
					} else {
						tagNode = new TagNode(accumulatorValue + "", opeart, Accumulator.getAccumulatorValue() + "",
								twoNode.getTagId(), parentNode, level);
					}
				} else {
					if (twoNode.getOpeart() != null) {
						tagNode = new TagNode(accumulatorValue + "", opeart, oneNode.getTagId(),
								Accumulator.getAccumulatorValue() + "", parentNode, level);
					} else {
						tagNode = new TagNode(accumulatorValue + "", opeart, oneNode.getTagId(), twoNode.getTagId(),
								parentNode, level);
					}

				}
				oneNode.parseNode(listHashMap, level + 1, accumulatorValue + "");
				twoNode.parseNode(listHashMap, level + 1, accumulatorValue + "");
			} else {
				Node oneNode = children.get(0);
				tagNode = new TagNode(oneNode.getTagId(), oneNode.getTagId(), parentNode, level);
			}
		} else {
			String tagId = this.getTagId();
			tagNode = new TagNode(tagId, tagId, parentNode, level);
		}
		List<TagNode> list = listHashMap.get(level);
		if (list == null) {
			list = new ArrayList<>();
			list.add(tagNode);
			listHashMap.put(level, list);
		} else {
			list.add(tagNode);
		}

	}
}

class User {

	public String name;
	public int age;
	@SerializedName(value = "emailAddress", alternate = { "email", "email_address" })
	public String emailAddress;

	public User(String name, int age, String emailAddress) {
		this.name = name;
		this.age = age;
		this.emailAddress = emailAddress;
	}

	public User() {
	}

	@Override
	public String toString() {
		return "User{" + "name='" + name + '\'' + ", age=" + age + ", emailAddress='" + emailAddress + '\'' + '}';
	}

}

class Result<T> {
	public int code;
	public String message;
	public T data;
}

class Accumulator {
	int value;

	private static Accumulator accumulator = new Accumulator(0);

	public static int getAccumulatorValue() {
		accumulator.setValue(accumulator.getValue() + 1);
		return accumulator.value;
	}

	public Accumulator(int value) {

		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
}
