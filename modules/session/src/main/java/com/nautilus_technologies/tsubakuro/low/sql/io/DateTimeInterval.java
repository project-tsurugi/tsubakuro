package com.nautilus_technologies.tsubakuro.low.sql.io;

import java.math.BigDecimal;

/**
 * Represents interval on calendar.
 */
public final class DateTimeInterval {

    private final int year;

    private final int month;

    private final int day;

    private final long nanoseconds;

    /**
     * @param year the year offset
     * @param month the month offset
     * @param day the day offset
     * @param nanoseconds the nano-seconds offset
     */
    public DateTimeInterval(int year, int month, int day, long nanoseconds) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.nanoseconds = nanoseconds;
    }

    /**
     * Returns the year offset.
     * @return the year offset
     */
    public int getYear() {
        return year;
    }

    /**
     * Returns the month offset.
     * @return the month offset
     */
    public int getMonth() {
        return month;
    }

    /**
     * Returns the day offset.
     * @return the day offset
     */
    public int getDay() {
        return day;
    }

    /**
     * Returns the nano-seconds offset.
     * @return the nano-seconds offset
     */
    public long getNanoseconds() {
        return nanoseconds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Integer.hashCode(year);
        result = prime * result + Integer.hashCode(month);
        result = prime * result + Integer.hashCode(day);
        result = prime * result + Long.hashCode(nanoseconds);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DateTimeInterval other = (DateTimeInterval) obj;
        return day == other.day
                && month == other.month
                && day == other.day
                && nanoseconds == other.nanoseconds;
    }

    @Override
    public String toString() {
        return String.format(
                "DateTimeInterval [year=%s, month=%s, day=%s, seconds=%s]",
                year,
                month,
                day,
                BigDecimal.valueOf(nanoseconds).scaleByPowerOfTen(-9).toPlainString());
    }
}
