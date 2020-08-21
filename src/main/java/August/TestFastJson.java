package August;

import com.alibaba.fastjson.JSONObject;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/8/23 15:44
 */
public class TestFastJson {
	public static void main(String[] args) {
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

		JSONObject.parseObject(value, InfoBean.class);

	}

	class InfoBean {
		private Node data;

        public Node getData() {
            return data;
        }

        public void setData(Node data) {
            this.data = data;
        }
    }
	class Node{

//        private Boolean boolean;


    }
}
