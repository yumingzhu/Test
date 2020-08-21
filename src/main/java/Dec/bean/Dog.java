package Dec.bean;

public class Dog {
    private  String name;
    private  int age;

    private Dog(Builder builder) {
        name = builder.name;
        age = builder.age;
    }


    public static final class Builder {
        private String name;
        private int age;

        public Builder() {
        }

        public Builder name(String val) {
            name = val;
            return this;
        }

        public Builder age(int val) {
            age = val;
            return this;
        }

        public Dog build() {
            return new Dog(this);
        }
    }
}
