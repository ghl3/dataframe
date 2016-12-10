package dataframe;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TableBuilder {

    private final String[] header;

    private List<String[]> rows;

    static final String COLUMN_SEPARATOR = " ";

    public TableBuilder(String indexName, Iterable<Object> columns) {

        List<String> names = Lists.newArrayList();

        names.add(formatObject(indexName));

        for (Object col: columns) {
            names.add(formatObject(col));
        }

        String[] header = new String[names.size()];
        header = names.toArray(header);
        this.header = header;

        rows = new LinkedList<String[]>();
    }

    public TableBuilder(Iterable<Object> columns) {
        this("idx", columns);
    }

    public TableBuilder addRow(Object idx, List<Object> row) {

        assert row.size() == this.header.length-1;

        List<String> formattedRow = new ArrayList<String>();

        formattedRow.add(formatObject(idx));

        for (Object item: row) {
            formattedRow.add(formatObject(item));
        }

        String[] cols = new String[row.size()];
        cols = formattedRow.toArray(cols);

        rows.add(cols);

        return this;
    }

    public static String formatObject(Object o) {
        if (o == null) {
            return "nil";
        } else {
            return o.toString();
        }
    }

    private int totalWidth() {
        int total = 0;
        for (int w: colWidths()) {
            total += w + COLUMN_SEPARATOR.length();
        }
        return total;
    }

    private int[] colWidths() {

        int numCols = header.length;

        int[] widths = new int[numCols];

        for(int colNum = 0; colNum < header.length; colNum++) {
            widths[colNum] = header[colNum].length();
        }

        for(String[] row : rows) {
            for(int colNum = 0; colNum < row.length; colNum++) {
                widths[colNum] = Math.max(widths[colNum], StringUtils.length(row[colNum]));
            }
        }

        return widths;
    }

    static void addLine(StringBuilder buf,
                               String[] line,
                               int[] colWidths) {
        for(int colNum = 0; colNum < line.length; colNum++) {
            buf.append(
                StringUtils.leftPad(
                    StringUtils.defaultString(
                        line[colNum]), colWidths[colNum]));
            buf.append(COLUMN_SEPARATOR);
        }

        buf.append('\n');
    }

    @Override
    public String toString() {

        StringBuilder buf = new StringBuilder();

        int[] colWidths = colWidths();

        addLine(buf, this.header, colWidths);

        for(String[] row: this.rows) {
            addLine(buf, row, colWidths);
        }

        return buf.toString();
    }
}

