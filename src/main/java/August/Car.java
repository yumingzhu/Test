package August;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/9/12 10:10
 */

public class Car {
	private Integer name;
	private Integer type;

	public Integer getName() {
		return name;
	}

	public void setName(Integer name) {
		this.name = name;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Car) {
			Car other = (Car) obj;
			//需要比较的字段相等，则这两个对象相等
			if (this.name.equals(other.name) && this.type == other.type) {
				return true;
			}
		}

		return false;
	}

	public Car(Integer name, Integer type) {
		this.name = name;
		this.type = type;
	}


    @Override
    public String toString() {
        return "Car{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }
}
