package May.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/6 17:14
 */
@Data
public class Tag implements Serializable {

	private int id;

	private String tag_id;

	private String name;

	private int pid;

	private String pids;

	private int status;

	private int type;

	private int output_length;


    public Tag(int id, String tag_id, String name, int pid, String pids, int status, int type, int output_length) {
        this.id = id;
        this.tag_id = tag_id;
        this.name = name;
        this.pid = pid;
        this.pids = pids;
        this.status = status;
        this.type = type;
        this.output_length = output_length;
    }

    public Tag() {
    }

    @Override
    public String toString() {
        return "Tag{" +
                "id=" + id +
                ", tag_id='" + tag_id + '\'' +
                ", name='" + name + '\'' +
                ", pid=" + pid +
                ", pids='" + pids + '\'' +
                ", status=" + status +
                ", type=" + type +
                ", output_length=" + output_length +
                '}';
    }
}
