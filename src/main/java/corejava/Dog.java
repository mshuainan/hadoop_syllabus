package corejava;

public class Dog implements Comparable<Dog> {
    private int legs;
    private int age;

    public Dog(int legs, int age) {
        this.legs = legs;
        this.age = age;
    }

    public int getLegs() {
        return legs;
    }

    public void setLegs(int legs) {
        this.legs = legs;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public int compareTo(Dog o) {
//        return Integer.valueOf(this.age).compareTo(Integer.valueOf(o.getAge()));
        return Integer.valueOf(this.legs).compareTo(Integer.valueOf(o.getLegs()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dog dog = (Dog) o;

        if (legs != dog.legs) return false;
        return age == dog.age;
    }

    @Override
    public int hashCode() {
        int result = legs;
        result = 31 * result + age;
        return result;
    }

    @Override
    public String toString() {
        return "demo.Dog{" +
                "legs=" + legs +
                ", age=" + age +
                '}';
    }
}
