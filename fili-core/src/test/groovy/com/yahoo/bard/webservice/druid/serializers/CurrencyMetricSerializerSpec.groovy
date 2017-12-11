package com.yahoo.bard.webservice.druid.serializers

import com.yahoo.bard.webservice.data.metric.CurrencyMetric

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule

import spock.lang.Specification

class CurrencyMetricSerializerSpec extends Specification{
    CurrencyMetric currencyMetric
    ObjectMapper mapper

    def setup() {
        mapper = new ObjectMapper()

        SimpleModule module = new SimpleModule()
        module.addSerializer(CurrencyMetric.class, new CurrencyMetricSerializer())
        mapper.registerModule(module)

        currencyMetric = new CurrencyMetric("revenue", "100", "USD")
    }

    def "Serialize currency metric"() {
        when:
        String result = mapper.writeValueAsString(currencyMetric)

        then:
        result.equals("{\"revenue\":100,\"currency\":\"USD\"}")
    }
}
