package com.tsurugidb.tsubakuro.examples.measurement;

public class Profile {
    public long warehouses;
    public long count;
    public long records;
    public long elapsed;
    public long commit;
    public long head;
    public long body;

    public Profile(long warehouses) {
    this.warehouses = warehouses;
    this.count = 0;
    this.records = 0;
    this.elapsed = 0;
    this.commit = 0;
    this.head = 0;
    this.body = 0;
    }
    long ns2us(long t) {
    return (t + 500) / 1000;
    }
    long div(long a, long b) {
    if (b == 0) {
        return a;
    }
    return a / b;
    }
    public void print() {
    System.out.println("==== time");
    System.out.printf("%d / %d = %d (us)\n", ns2us(elapsed), count, ns2us(elapsed / count));
    if (head > 0) {
        System.out.printf("%d %d %d / %d = %d %d %d (us)\n", ns2us(head), ns2us(body), ns2us(commit),
                  count,
                  ns2us(head / count), ns2us(body / count), ns2us(commit / count));
    } else {
        System.out.printf("%d %d / %d = %d %d (us)\n", ns2us(body), ns2us(commit),
                  count,
                  ns2us(body / count), ns2us(commit / count));
    }
    if (records > 0) {
        System.out.println("==== records read");
        System.out.printf("%d / %d = %d (records)\n", records, count, (records / count));
    }
    }
}
