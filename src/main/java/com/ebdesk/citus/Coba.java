package com.ebdesk.citus;

import java.util.concurrent.TimeUnit;

public class Coba {

    public static void main(String[] args) {
        long timestamp1 = Long.valueOf("1555300575000");
        long timestamp2 = Long.valueOf("1555300580000");
        long diffInDays = getDateDiff(timestamp1, timestamp2, TimeUnit.MILLISECONDS);
        System.out.println(diffInDays);

    }

    public static long getDateDiff(long timeUpdate, long timeNow, TimeUnit timeUnit)
    {
        long diffInMillies = Math.abs(timeNow- timeUpdate);
        return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
    }

}
