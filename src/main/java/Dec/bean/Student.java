package Dec.bean;

import scala.math.Ordered;

import java.io.Serializable;
import java.util.Comparator;

public class Student implements Serializable, Ordered<Student> {
    private String name;
    private int chinese;
    private int math;

    public Student() {
    }

    private Student(Builder builder) {
        name = builder.name;
        chinese = builder.chinese;
        math = builder.math;
    }

    public String getName() {
        return name;
    }

    public int getChinese() {
        return chinese;
    }

    public int getMath() {
        return math;
    }



    public static final class Builder {
        private String name;
        private int chinese;
        private int math;

        public Builder() {
        }

        public Builder name(String val) {
            name = val;
            return this;
        }

        public Builder chinese(int val) {
            chinese = val;
            return this;
        }

        public Builder math(int val) {
            math = val;
            return this;
        }

        public Student build() {
            return new Student(this);
        }

    }
    @Override
    public int compare(Student that) {
        if(this.getChinese()-that.getChinese()!=0){
            return  this.getChinese()-that.getChinese();
        }else{
            return  this.getMath()-that.getMath();
        }
    }

    @Override
    public boolean $less(Student that) {
        if(this.getChinese()<that.getChinese()){
            return  true;
        }else if(this.getChinese()==that.getChinese() && this.getMath()<that.getMath()){
            return  true;
        }
        return  false;
    }

    @Override
    public boolean $greater(Student that) {
        if(this.getChinese()>that.getChinese()){
            return  true;
        }else if(this.getChinese()==that.getChinese() && this.getMath()>that.getMath()){
                                return  true;
        }
        return  false;
    }

    @Override
    public boolean $less$eq(Student that) {
        if($less(this)||this.getChinese()==that.getChinese()){
            return  true;
        }
        return  false;
    }

    @Override
    public boolean $greater$eq(Student that) {
        if($greater(this)||this.getChinese()==that.getChinese()){
            return  true;
        }
        return  false;
    }

    @Override
    public int compareTo(Student that) {
        if(this.getChinese()-that.getChinese()!=0){
            return  this.getChinese()-that.getChinese();
        }else{
            return  this.getMath()-that.getMath();
        }
    }

}
