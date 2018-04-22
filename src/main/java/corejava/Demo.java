package corejava;

import java.util.TreeSet;

public class Demo {
    public static void main(String[] args) {
        TreeSet<Dog> treeSet = new TreeSet<>();
        System.out.println(treeSet.add(new Dog(4, 4)));
        System.out.println(treeSet.add(new Dog(4, 3)));

        System.out.println(treeSet);
    }
}
