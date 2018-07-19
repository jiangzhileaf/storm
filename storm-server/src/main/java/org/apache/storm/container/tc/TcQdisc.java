package org.apache.storm.container.tc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TcQdisc {

    private static final String ROOT = "root";

    private String id;
    private String type;
    private String networkCard;
    private Boolean isRoot;
    private Map<String, String> props;

    private List<TcClass> classes;

    public TcQdisc() {
    }

    public String getId() {
        return id;
    }

    /**
     * get id in decimal.
     */
    public Integer getIdIndecimal() {

        String[] ids = id.split(":");
        String major = ids[0];
        String minor = ids[1];

        if (minor.isEmpty()) {
            BigInteger a = new BigInteger(major, 16);
            a.shiftLeft(16);
            return a.intValue();
        } else {
            BigInteger a = new BigInteger(major, 16);
            BigInteger i = new BigInteger(minor, 16);
            a = a.shiftLeft(16);
            return a.intValue() + i.intValue();
        }
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getNetworkCard() {
        return networkCard;
    }

    public void setNetworkCard(String networkCard) {
        this.networkCard = networkCard;
    }

    public List<TcClass> getClasses() {
        return classes;
    }

    public void setClasses(List<TcClass> classes) {
        this.classes = classes;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public Boolean isRoot() {
        return isRoot;
    }

    public void setRoot(Boolean root) {
        isRoot = root;
    }

    /**
     * parse the cmd result to object.
     */
    public static List<TcQdisc> parse(String qdiscArrayStr) {

        String[] qdiscStrs = qdiscArrayStr.split(System.lineSeparator());
        List<TcQdisc> result = new ArrayList<>();

        for (String qdiscStr : qdiscStrs) {
            String[] field = qdiscStr.split(" ");

            boolean isRoot = ROOT.equals(field[5]);

            Map<String, String> props = new LinkedHashMap<>();
            int propStartIndex = isRoot ? 7 : 8;
            for (int i = propStartIndex; i < field.length; i = i + 2) {
                props.put(field[i - 1], field[i]);
            }

            String type = field[1];
            String id = field[2];
            String networkCard = field[4];

            TcQdisc qdisc = new TcQdisc();
            qdisc.setId(id);
            qdisc.setType(type);
            qdisc.setRoot(isRoot);
            qdisc.setNetworkCard(networkCard);
            qdisc.setProps(props);

            result.add(qdisc);

        }

        return result;
    }

    /**
     * convert object to string in tree.
     */
    public String printAsTree() {
        if (isRoot) {

            String lineSeparator = System.lineSeparator();

            StringBuilder sb = new StringBuilder();
            sb.append("qdisc ").append(type).append(" ").append(id).append(" dev ").append(networkCard).append(" root");
            for (Map.Entry<String, String> entry : props.entrySet()) {
                sb.append(" ").append(entry.getKey()).append(" ").append(entry.getValue());
            }

            sb.append(lineSeparator);

            for (TcClass tcClass : classes) {
                sb.append(tcClass.printAsTree());
            }

            sb.append(lineSeparator);

            return sb.toString();
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        return printAsTree();
    }
}
