package dianduidian;

import java.io.Serializable;

public class User implements Serializable {

    private static final long serialVersionUID= -9022698990903328142L;

    private int id;
    private String name;
    private String addr;
    private String sex;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", addr='" + addr + '\'' +
                ", sex='" + sex + '\'' +
                '}';
    }
}
