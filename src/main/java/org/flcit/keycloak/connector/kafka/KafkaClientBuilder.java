/*
 * Copyright 2002-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flcit.keycloak.connector.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.keycloak.Config.Scope;

import org.flcit.commons.core.util.ObjectUtils;
import org.flcit.commons.core.util.PropertyUtils;
import org.flcit.commons.core.util.StringUtils;

public final class KafkaClientBuilder {

    private static final String PREFIX_JAAS = "sasl.jaas.";
    private static final String ACKS_DEFAULT = "1";

    private KafkaClientBuilder() { }

    public static KafkaProducer<String, Object> buildProducer(Properties properties, String prefix) {
        ProducerConfig.main(getCommonConfig());
        return buildKafkaProducer(getProducerProperties(properties, prefix));
    }

    @SuppressWarnings("unchecked")
    private static KafkaProducer<String, Object> buildKafkaProducer(final Properties properties) {
        return new KafkaProducer<>(properties, (Serializer<String>) getSerializer(properties, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), (Serializer<Object>) getSerializer(properties, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @SuppressWarnings("unchecked")
    private static Serializer<? extends Object> getSerializer(final Properties properties, String property) {
        final String serializerClassConfig = properties.getProperty(property);
        if (serializerClassConfig == null) {
            return new StringSerializer();
        }
        try {
            return (Serializer<? extends Object>) Class.forName(serializerClassConfig).newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Properties getProducerProperties(Properties properties, String prefix) {
        final Properties targetProperties = getProperties(properties, getAllProducerConfig(), prefix, prefix + "producer.");
        putCommonConfig(properties, targetProperties, prefix);
        return defaultConfig(targetProperties);
    }

    private static Properties getProperties(Properties properties, Collection<String> configNames, String... prefixes) {
        final Properties kafkaProperties = new Properties();
        for (Entry<Object, Object> entry : properties.entrySet()) {
            final String targetProperty = getTargetProperty(StringUtils.convert(entry.getKey()), configNames, prefixes);
            if (targetProperty != null) {
                kafkaProperties.put(targetProperty, entry.getValue());
            }
        }
        return kafkaProperties;
    }

    private static Map<String, Object> getProperties(Properties properties, boolean removePrefix, String... prefixes) {
        final Map<String, Object> findProperties = new HashMap<>();
        for (Entry<Object, Object> entry : properties.entrySet()) {
            final String property = StringUtils.convert(entry.getKey());
            final String targetProperty = getTargetProperty(property, prefixes);
            if (targetProperty != null) {
                findProperties.put(removePrefix ? targetProperty : property, entry.getValue());
            }
        }
        return findProperties;
    }

    private static String getTargetProperty(String property, Collection<String> configNames, String... prefixes) {
        if (property == null) {
            return null;
        }
        for (String name: configNames) {
            for (String prefix: prefixes) {
                if (property.equals(prefix + name)) {
                    return name;
                }
            }
        }
        return null;
    }

    private static String getTargetProperty(String property, String... prefixes) {
        if (property == null) {
            return null;
        }
        for (String prefix: prefixes) {
            if (property.startsWith(prefix)) {
                return property.substring(prefix.length());
            }
        }
        return null;
    }

    private static List<String> getAllProducerConfig() {
        final List<String> config = new ArrayList<>(ProducerConfig.configNames());
        Collections.addAll(config, getCommonConfig());
        return config;
    }

    private static String[] getCommonConfig() {
        return new String[] {
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                CommonClientConfigs.CLIENT_ID_CONFIG,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                SaslConfigs.SASL_MECHANISM,
                SaslConfigs.SASL_KERBEROS_SERVICE_NAME
        };
    }

    private static Properties putCommonConfig(Properties properties, Properties targetProperties, String prefix) {
        putJaasConfig(properties, targetProperties, prefix);
        return targetProperties;
    }

    private static Properties putJaasConfig(Properties properties, Properties targetProperties, String prefix) {
        if (Boolean.parseBoolean((String) properties.get(prefix + PREFIX_JAAS + "enabled"))) {
            final Map<String, Object> jassOptions = getProperties(properties, true, prefix + PREFIX_JAAS + "options.");
            final StringBuilder sb = new StringBuilder(ObjectUtils.getOrDefault(jassOptions.remove("loginModule"), "com.sun.security.auth.module.Krb5LoginModule").toString());
            sb.append(" required");
            for (Entry<String, Object> entry: jassOptions.entrySet()) {
                sb.append(' ');
                sb.append(entry.getKey());
                sb.append('=');
                sb.append(entry.getValue());
            }
            sb.append(';');
            targetProperties.put(SaslConfigs.SASL_JAAS_CONFIG, sb.toString());
        }
        return targetProperties;
    }

    public static KafkaProducer<String, Object> buildProducer(Scope config, String prefix) {
        return buildKafkaProducer(getProducerProperties(config, prefix));
    }

    private static Properties getProducerProperties(Scope config, String prefix) {
        final Properties properties = new Properties();
        for (String property: config.getPropertyNames()) {
            if (property.startsWith(prefix)) {
                properties.put(property, config.get(property));
            }
        }
        return properties;
    }

    private static Properties defaultConfig(Properties properties) {
        properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, ACKS_DEFAULT);
        properties.computeIfPresent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, (key, value) -> PropertyUtils.toList((String) value));
        return properties;
    }

}
