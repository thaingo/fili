package com.yahoo.bard.webservice.data.metric;

public class CurrencyMetric {
    private final String currencyName;
    private final String currencyValue;
    private final String currencyCode;

    /**
     * Constructor.
     *
     * @param currencyName  The name of the money field (i.e. cost, revenue)
     * @param currencyValue  The amount of money
     * @param currencyCode  The currency the money
     */
    public CurrencyMetric(
            String currencyName,
            String currencyValue,
            String currencyCode
    ) {
        this.currencyName = currencyName;
        this.currencyValue = currencyValue;
        this.currencyCode = currencyCode;
    }

    public String getCurrencyName() {
        return currencyName;
    }

    public String getCurrencyValue() {
        return currencyValue;
    }

    public String getCurrencyCode() {
        return currencyCode;
    }
}
