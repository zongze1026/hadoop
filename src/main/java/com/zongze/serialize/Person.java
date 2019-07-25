package com.zongze.serialize;

/**
 * Create By xzz on 2019/7/24
 */
public class Person {
    private String name;
    private Integer id;
    private boolean marry;

    public Person() {
    }

    public Person(String name, Integer id, boolean marry) {
        this.name = name;
        this.id = id;
        this.marry = marry;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public boolean isMarry() {
        return marry;
    }

    public void setMarry(boolean marry) {
        this.marry = marry;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", marry=" + marry +
                '}';
    }


    public static void main(String[] args) {
        byte i = -2;
        System.out.println(Integer.toBinaryString(-2));
        System.out.println(0xff);
        System.out.println(0xFF);

    }

}
