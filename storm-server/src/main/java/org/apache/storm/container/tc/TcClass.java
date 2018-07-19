package org.apache.storm.container.tc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TcClass {

    private static final String ROOT = "root";
    private static final String LEAF = "leaf";

    private String id;
    private String type;
    private Boolean isRoot;

    private Map<String, String> props;
    private TcQdisc leaf;

    public TcClass() {
    }

    public String getId() {
        return id;
    }

    /**
     * get id in decimal.
     */
    public static Long getIdIndecimal(String id) {

        String[] ids = id.split(":");
        String major = ids[0];
        String minor = ids[1];

        if (minor.isEmpty()) {
            BigInteger a = new BigInteger(major, 16);
            a.shiftLeft(16);
            return a.longValue();
        } else {
            BigInteger a = new BigInteger(major, 16);
            BigInteger i = new BigInteger(minor, 16);
            a = a.shiftLeft(16);
            return a.longValue() + i.longValue();
        }
    }

    /**
     * get id in string, format:major:minor.
     */
    public static String getIdInStr(long decimalId) {

        BigInteger did = new BigInteger(String.valueOf(decimalId));

        String major = String.valueOf(did.shiftRight(16).longValue());
        String minor = String.valueOf(did.and(BigInteger.valueOf(0xffffL)).longValue());

        return String.format("%s:%s", major, minor);
    }

    public static void main(String[] args) {
        System.out.println(getIdInStr(65544));
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

    public Boolean getRoot() {
        return isRoot;
    }

    public void setRoot(Boolean root) {
        isRoot = root;
    }

    /**
     * parse the cmd result to object.
     */
    public static List<TcClass> parse(List<TcQdisc> qdiscs, String classArrayStr) {

        if (classArrayStr == null || classArrayStr.isEmpty()) {
            return new ArrayList<>();
        }

        String[] classStrs = classArrayStr.split(System.lineSeparator());
        List<TcClass> result = new ArrayList<>();

        for (String classStr : classStrs) {
            String[] field = classStr.split(" ");

            boolean isRoot = ROOT.equals(field[3]);

            Map<String, String> props = new LinkedHashMap<>();
            int propStartIndex = isRoot ? 3 : 4;
            for (int i = propStartIndex; i < field.length; i = i + 2) {
                props.put(field[i - 1], field[i]);
            }

            String leafId = props.get(LEAF);
            TcQdisc leaf = null;
            if (leafId != null) {
                leaf = findLeaf(qdiscs, leafId);
            }

            String type = field[1];
            String id = field[2];

            TcClass tcClass = new TcClass();
            tcClass.setId(id);
            tcClass.setType(type);
            tcClass.setLeaf(leaf);
            tcClass.setRoot(isRoot);
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

    /**
     * convert object to string in tree.
     */
    public String printAsTree() {
        String lineSeparator = System.lineSeparator();

        StringBuilder sb = new StringBuilder();
        sb.append("|").append(lineSeparator);
        sb.append("+- class ").append(type).append(" ").append(id);
        if (this.isRoot) {
            sb.append(" root");
        }

        for (Map.Entry<String, String> entry : props.entrySet()) {
            sb.append(" ").append(entry.getKey()).append(" ").append(entry.getValue());
        }

        if (leaf != null) {
            sb.append(lineSeparator);
            sb.append("|  |").append(lineSeparator);
            sb.append("|  \\- ");
            sb.append("qdisc ").append(leaf.getType()).append(" ").append(leaf.getId())
                    .append(" dev ").append(leaf.getNetworkCard()).append(" root");
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
