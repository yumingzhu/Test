package Dec.bean;

public class Score {
    private  Integer chinese;
    private  Integer math;

    public Score(Integer chinese, Integer math) {
        this.chinese = chinese;
        this.math = math;
    }

    @Override
    public String toString() {
        return "Score{" +
                "chinese=" + chinese +
                ", math=" + math +
                '}';
    }

    public Integer getChinese() {
        return chinese;
    }

    public void setChinese(Integer chinese) {
        this.chinese = chinese;
    }

    public Integer getMath() {
        return math;
    }

    public void setMath(Integer math) {
        this.math = math;
    }
}
