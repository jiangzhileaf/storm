package org.apache.storm.container.tc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TcClass {

    private String id;
    private String type;

    private Map<String, String> props;
    private TcQdisc leaf;

    public TcClass() {
    }

    public String getId() {
        return id;
    }

    public static Long getIdIndecimal(String id) {

        String[] ids = id.split(":");
        String major = ids[0];
        String minor = ids[1];

        if (minor.isEmpty()) {
            BigInteger a = new BigInteger(major);
            a.shiftLeft(16);
            return a.longValue();
        } else {
            BigInteger a = new BigInteger(major);
            BigInteger i = new BigInteger(minor);
            a = a.shiftLeft(16);
            return a.longValue() + i.longValue();
        }
    }

    public static String getIdInStr(long decimalId) {

        BigInteger dId = new BigInteger(String.valueOf(decimalId));

        String major = String.valueOf(dId.shiftRight(16).longValue());
        String minor = String.valueOf(dId.and(BigInteger.valueOf(0xffffL)).longValue());

        return String.format("%s:%s", major, minor);
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

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public TcQdisc getLeaf() {
        return leaf;
    }

    public void setLeaf(TcQdisc leaf) {
        this.leaf = leaf;
    }

    public static List<TcClass> parse(List<TcQdisc> qdiscs, String classArrayStr) {

        String[] classStrs = classArrayStr.split(System.lineSeparator());
        List<TcClass> result = new ArrayList<>();

        /*
        class htb 1:1 root leaf 2: prio 0 rate 1000Mbit ceil 1000Mbit burst 1375b cburst 1375b
        class htb 1:2 root leaf 3: prio 1 rate 1000Mbit ceil 1000Mbit burst 1375b cburst 1375b
        class htb 1:3 root leaf 4: prio 2 rate 1000Mbit ceil 1000Mbit burst 1375b cburst 1375b
        class htb 1:4 root prio 1 rate 50000Kbit ceil 1000Mbit burst 1600b cburst 32000b
        class htb 1:5 root prio 1 rate 100000Kbit ceil 1000Mbit burst 1600b cburst 16000b
        class htb 1:6 root prio 1 rate 200000Kbit ceil 1000Mbit burst 1600b cburst 8000b*/

        for (String classStr : classStrs) {
            String[] field = classStr.split(" ");

            String type = field[1];
            String id = field[2];
            boolean hasLeaf = hasLeaf(field[4]);
            TcQdisc leaf = null;

            if (hasLeaf) {
                leaf = findLeaf(qdiscs, field[5]);
            }

            Map<String, String> props = new HashMap<>();

            int propStartIndex = hasLeaf ? 6 : 4;
            for (int i = propStartIndex; i < field.length; i = i + 2) {
                props.put(field[i], field[i + 1]);
            }

            TcClass tcClass = new TcClass();
            tcClass.setId(id);
            tcClass.setType(type);
            tcClass.setLeaf(leaf);
            tcClass.setProps(props);

            result.add(tcClass);
        }

        return result;
    }

    private static TcQdisc findLeaf(List<TcQdisc> qdiscs, String id) {
        for (int i = 0; i < qdiscs.size(); i++) {
            if (id.equals(qdiscs.get(i).getId())) {
                return qdiscs.get(i);
            }
        }
        return null;
    }

    private static boolean hasLeaf(String str) {
        return "leaf".equals(str);
    }

    public String printAsTree() {
        String lineSeparator = System.lineSeparator();

        StringBuilder sb = new StringBuilder();
        sb.append("|").append(lineSeparator);
        sb.append("+- class ").append(type).append(" ").append(id).append(" root");
        for (Map.Entry<String, String> entry : props.entrySet()) {
            sb.append(" ").append(entry.getKey()).append(" ").append(entry.getValue());
        }

        if (leaf != null) {
            sb.append(lineSeparator);
            sb.append("|  |").append(lineSeparator);
            sb.append("|  \\- ");
            sb.append("qdisc ").append(leaf.getType()).append(" ").append(leaf.getId()).append(" dev ").append(leaf.getNetworkCard()).append(" root");
            for (Map.Entry<String, String> entry : leaf.getProps().entrySet()) {
                sb.append(" ").append(entry.getKey()).append(" ").append(entry.getValue());
            }
        }

        sb.append(lineSeparator);

        return sb.toString();
    }

    @Override
    public String toString() {
        return printAsTree();
    }
}
