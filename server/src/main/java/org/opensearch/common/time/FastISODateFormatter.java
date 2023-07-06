/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

public class FastISODateFormatter implements DateFormatter {

    private final JavaDateFormatter javaDateFormatter;
    private final boolean local;

    public FastISODateFormatter(JavaDateFormatter formatter, boolean local) {
        this.javaDateFormatter = formatter;
        this.local = local;
    }

    @Override
    public TemporalAccessor parse(String input) {
        if (local) {
            return FastDTParser.parseLocalDateTime(input, javaDateFormatter);
        }
        return FastDTParser.
            parse(input);
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        return javaDateFormatter.withZone(zoneId);
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
//        if (Locale.ROOT.equals(locale)) {
//            return this;
//        }
        return this.javaDateFormatter.withLocale(locale);
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return this.javaDateFormatter.format(accessor);
    }

    @Override
    public String pattern() {
        return javaDateFormatter.pattern();
    }

    @Override
    public Locale locale() {
        return Locale.ROOT;
    }

    @Override
    public ZoneId zone() {
        return this.javaDateFormatter.zone();
    }

    @Override
    public DateMathParser toDateMathParser() {
        return this.javaDateFormatter.toDateMathParser();
    }

    public static void main(String[] args) {
        System.out.println(Double.toString(1.239 ));
        System.out.println(com.fasterxml.jackson.core.io.schubfach.DoubleToDecimal.toString(1.239 ));
    }
}
