package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.metric.CurrencyMetric;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class CurrencyMetricSerializer extends StdSerializer<CurrencyMetric> {

    public CurrencyMetricSerializer() {
        this(null);
    }

    public CurrencyMetricSerializer(Class<CurrencyMetric> t) {
        super(t);
    }

    @Override
    public void serialize(
            final CurrencyMetric value, final JsonGenerator gen, final SerializerProvider provider
    ) throws IOException {
        gen.writeStartObject();
        gen.writeNumberField(value.getCurrencyName(), Integer.parseInt(value.getCurrencyValue()));
        gen.writeStringField("currency", value.getCurrencyCode());
        gen.writeEndObject();
    }
}
