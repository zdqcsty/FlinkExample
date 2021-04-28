package xuwei.tech.suanzi.aggr;

public class Score {

    private String name;
    private String course;
    private int score;

    @Override
    public String toString() {
        return "Score{" +
                "name='" + name + '\'' +
                ", course='" + course + '\'' +
                ", score=" + score +
                '}';
    }

    public Score(String name, String course, int score){
        this.name=name;
        this.course=course;
        this.score=score;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

}
