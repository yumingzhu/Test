package Dec.bean;

import java.io.Serializable;

public class Person  implements Serializable {
    private String name;

    private Person(Builder builder) {
        name = builder.name;
    }


    public static final class Builder {
        private String name;

        public Builder() {
        }

        public Builder name(String val) {
            name = val;
            return this;
        }

        public Person build() {
            return new Person(this);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
