package com.dwh.beam;

import java.io.Serializable;
import java.util.Objects;

/**
 * A BigQuery column type, including the precision and scale that make a decimal column correct.
 *
 * <p>Precision and scale are not decoration. BigQuery's {@code NUMERIC} is fixed at precision 38,
 * scale 9. An Avro decimal with scale 10 mapped onto {@code NUMERIC} loses a digit -- quietly, in a
 * money column, with no error anywhere. For a trading firm that is the worst possible failure mode,
 * so the scale decides the type rather than being an afterthought.
 */
public final class BqType implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final BqType STRING = simple("STRING");
    public static final BqType INT64 = simple("INT64");
    public static final BqType FLOAT64 = simple("FLOAT64");
    public static final BqType BOOL = simple("BOOL");
    public static final BqType BYTES = simple("BYTES");
    public static final BqType TIMESTAMP = simple("TIMESTAMP");
    public static final BqType DATE = simple("DATE");

    private final String name;
    private final Integer precision;
    private final Integer scale;

    private BqType(String name, Integer precision, Integer scale) {
        this.name = name;
        this.precision = precision;
        this.scale = scale;
    }

    public static BqType simple(String name) {
        return new BqType(name, null, null);
    }

    public static BqType numeric(int precision, int scale) {
        return new BqType("NUMERIC", precision, scale);
    }

    public static BqType bigNumeric(int precision, int scale) {
        return new BqType("BIGNUMERIC", precision, scale);
    }

    public String name() { return name; }
    public Integer precision() { return precision; }
    public Integer scale() { return scale; }

    /** The type as it would appear in an ALTER TABLE ADD COLUMN. */
    public String ddl() {
        if (precision == null) {
            return name;
        }
        return name + "(" + precision + ", " + scale + ")";
    }

    /**
     * Can a column of this type hold a value of {@code incoming} without loss?
     *
     * <p>BigQuery permits a narrow set of widenings on an existing column:
     * INT64 to NUMERIC, BIGNUMERIC or FLOAT64, and NUMERIC to BIGNUMERIC or FLOAT64. Nothing widens
     * to or from STRING. This is exactly why guessing a type is worse than refusing to: a column
     * created as STRING can never become a number, and fixing it means recreating the table.
     */
    public boolean canAccept(BqType incoming) {
        if (this.equals(incoming)) {
            return true;
        }
        if (name.equals(incoming.name)) {
            // same family: only safe if we are at least as precise
            if (precision == null || incoming.precision == null) {
                return true;
            }
            return precision >= incoming.precision && scale >= incoming.scale;
        }
        switch (incoming.name) {
            case "INT64":
                return name.equals("NUMERIC") || name.equals("BIGNUMERIC") || name.equals("FLOAT64");
            case "NUMERIC":
                return name.equals("BIGNUMERIC") || name.equals("FLOAT64");
            default:
                return false;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BqType)) {
            return false;
        }
        BqType that = (BqType) other;
        return name.equals(that.name)
                && Objects.equals(precision, that.precision)
                && Objects.equals(scale, that.scale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, precision, scale);
    }

    @Override
    public String toString() {
        return ddl();
    }
}
