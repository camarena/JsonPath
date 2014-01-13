/*
 * Copyright 2011 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jayway.jsonpath.spi.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.internal.Utils;
import com.jayway.jsonpath.spi.MappingProvider;
import com.jayway.jsonpath.spi.Mode;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.*;



/**
 * @author Kalle Stenflo
 */
public class JacksonProvider extends AbstractJsonProvider implements MappingProvider{

    private ObjectMapper objectMapper = new ObjectMapper();

	enum SupportedTypes {
		BIG_DECIMAL,
		BOOLEAN,
		BYTE_ARRAY,
		DOUBLE,
		FLOAT,
		INTEGER,
		JSON_NODE,
		LONG,
		SHORT,
		STRING
	}

	private static final Map<String, SupportedTypes> SUPPORTED_TYPES;

	static {
		SUPPORTED_TYPES = new HashMap<String, SupportedTypes>(10);
		SUPPORTED_TYPES.put(BigDecimal.class.getCanonicalName(), SupportedTypes.BIG_DECIMAL);
		SUPPORTED_TYPES.put(Boolean.class.getCanonicalName(), SupportedTypes.BOOLEAN);
		SUPPORTED_TYPES.put(Byte[].class.getCanonicalName(), SupportedTypes.BYTE_ARRAY);
		SUPPORTED_TYPES.put(Double.class.getCanonicalName(), SupportedTypes.DOUBLE);
		SUPPORTED_TYPES.put(Float.class.getCanonicalName(), SupportedTypes.FLOAT);
		SUPPORTED_TYPES.put(Integer.class.getCanonicalName(), SupportedTypes.INTEGER);
		SUPPORTED_TYPES.put(JsonNode.class.getCanonicalName(), SupportedTypes.JSON_NODE);
		SUPPORTED_TYPES.put(Long.class.getCanonicalName(), SupportedTypes.LONG);
		SUPPORTED_TYPES.put(Short.class.getCanonicalName(), SupportedTypes.SHORT);
		SUPPORTED_TYPES.put(String.class.getCanonicalName(), SupportedTypes.STRING);
	}

	@Override
	public Object clone(final Object obj) {
		if (obj instanceof JsonNode)
			return JsonNode.class.cast(obj).deepCopy();
		return Utils.clone((Serializable) obj);
	}

	@Override
	public boolean isContainer(final Object obj) {
		return obj instanceof ContainerNode || super.isContainer(obj);
	}

	@Override
	public boolean isArray(final Object obj) {
		return obj instanceof ArrayNode || super.isArray(obj);
	}

	@Override
	public Object getProperty(final Object obj, final Object key) {
		if (obj instanceof ObjectNode)
			return ObjectNode.class.cast(obj).get(String.class.cast(key));
		else if (obj instanceof ArrayNode) {
			final int index = key instanceof Integer ? Integer.class.cast(key).intValue() : Integer
					.parseInt(String.class.cast(key));
			return ArrayNode.class.cast(obj).get(index);
		} else
			return super.getProperty(obj, key);
	}

	@Override
	public void setProperty(final Object obj, final Object key, final Object value) throws InvalidJsonException {
		if (obj instanceof ObjectNode) {
			final SupportedTypes type = SUPPORTED_TYPES.get(value.getClass().getCanonicalName());
			if (type == null)
				throw new InvalidJsonException("Unsupported type");
			final String keyAsString = String.class.cast(key);
			final ObjectNode objectNode = ObjectNode.class.cast(obj);
			switch (type) {
				case BIG_DECIMAL:
					objectNode.put(keyAsString, BigDecimal.class.cast(value));
					break;
				case BOOLEAN:
					objectNode.put(keyAsString, Boolean.class.cast(value));
					break;
				case BYTE_ARRAY:
					objectNode.put(keyAsString, byte[].class.cast(value));
					break;
				case DOUBLE:
					objectNode.put(keyAsString, Double.class.cast(value));
					break;
				case FLOAT:
					objectNode.put(keyAsString, Float.class.cast(value));
					break;
				case INTEGER:
					objectNode.put(keyAsString, Integer.class.cast(value));
					break;
				case JSON_NODE:
					objectNode.put(keyAsString, JsonNode.class.cast(value));
					break;
				case LONG:
					objectNode.put(keyAsString, Long.class.cast(value));
					break;
				case SHORT:
					objectNode.put(keyAsString, Short.class.cast(value));
					break;
				case STRING:
					objectNode.put(keyAsString, String.class.cast(value));
					break;
			}
		} else if (obj instanceof ArrayNode) {
			final SupportedTypes type = SUPPORTED_TYPES.get(value.getClass().getCanonicalName());
			if (type == null)
				throw new InvalidJsonException("Unsupported type");
			final int index = key instanceof Integer ? Integer.class.cast(key).intValue() : Integer
					.parseInt(String.class.cast(key));
			final ArrayNode arrayNode = ArrayNode.class.cast(obj);
			switch (type) {
				case BIG_DECIMAL:
					arrayNode.insert(index, BigDecimal.class.cast(value));
					break;
				case BOOLEAN:
					arrayNode.insert(index, Boolean.class.cast(value));
					break;
				case BYTE_ARRAY:
					arrayNode.insert(index, byte[].class.cast(value));
					break;
				case DOUBLE:
					arrayNode.insert(index, Double.class.cast(value));
					break;
				case FLOAT:
					arrayNode.insert(index, Float.class.cast(value));
					break;
				case INTEGER:
					arrayNode.insert(index, Integer.class.cast(value));
					break;
				case JSON_NODE:
					arrayNode.insert(index, JsonNode.class.cast(value));
					break;
				case LONG:
					arrayNode.insert(index, Long.class.cast(value));
					break;
				case SHORT:
					arrayNode.insert(index, Short.class.cast(value));
					break;
				case STRING:
					arrayNode.insert(index, String.class.cast(value));
					break;
			}
		} else
			super.setProperty(obj, key, value);
	}

	@Override
	public Collection<String> getPropertyKeys(final Object obj) {
		if (obj instanceof ObjectNode) {
			final ObjectNode objectNode = ObjectNode.class.cast(obj);
			final Collection<String> fields = new ArrayList<String>(objectNode.size());
			final Iterator<String> iterator = objectNode.fieldNames();
			while (iterator.hasNext())
				fields.add(iterator.next());
			return fields;
		} else if (obj instanceof ArrayNode) {
			final ArrayNode arrayNode = ArrayNode.class.cast(obj);
			final Collection<String> fields = new ArrayList<String>(arrayNode.size());
			for (int i = 0; i < arrayNode.size(); i++)
				fields.add(String.valueOf(i));
			return fields;
		} else
			return super.getPropertyKeys(obj);
	}

	@Override
	public int length(final Object obj) {
		if (obj instanceof ContainerNode)
			return ContainerNode.class.cast(obj).size();
		return super.length(obj);
	}

	@Override
	public Iterable<Object> toIterable(final Object obj) {
		if (obj instanceof ObjectNode) {
			return new ObjectIterable() {

				@Override
				protected Iterator<?> getBaseIterator() {
					return ObjectNode.class.cast(obj).elements();
				}
			};
		} else if (obj instanceof ArrayNode) {
			return new ObjectIterable() {

				@Override
				protected Iterator<?> getBaseIterator() {
					return ArrayNode.class.cast(obj).iterator();
				}
			};
		} else
			return super.toIterable(obj);
	}

	@Override
	public boolean isMap(final Object obj) {
		return obj instanceof ObjectNode || super.isMap(obj);
	}


	@Override
    public Mode getMode() {
        return Mode.STRICT;
    }

	@Override
	public JsonNode parse(final String json) throws InvalidJsonException {
		try {
			return objectMapper.readValue(json, JsonNode.class);
		} catch (IOException e) {
			throw new InvalidJsonException(e);
		}
	}

	@Override
	public JsonNode parse(final Reader jsonReader) throws InvalidJsonException {
		try {
			return objectMapper.readValue(jsonReader, JsonNode.class);
		} catch (IOException e) {
			throw new InvalidJsonException(e);
		}
	}

	@Override
	public JsonNode parse(final InputStream jsonStream) throws InvalidJsonException {
		try {
			return objectMapper.readValue(jsonStream, JsonNode.class);
		} catch (IOException e) {
			throw new InvalidJsonException(e);
		}
	}

	@Override
	public String toJson(final Object obj) {
		if (obj instanceof JsonNode)
			return obj.toString();
		final StringWriter writer = new StringWriter();
		try {
			final JsonGenerator jsonGenerator = objectMapper.getJsonFactory().createJsonGenerator(writer);
			objectMapper.writeValue(jsonGenerator, obj);
			writer.close();
			return writer.getBuffer().toString();
		} catch (IOException e) {
			throw new InvalidJsonException();
		}
	}

	@Override
	public ObjectNode createMap() {
		return new ObjectNode(JsonNodeFactory.instance);
	}

	@Override
	public ArrayNode createArray() {
		return new ArrayNode(JsonNodeFactory.instance);
	}

	//-------------------------------------------------------------------
    //
    // Mapping provider
    //
    //-------------------------------------------------------------------

	@Override
	public <T> T convertValue(final Object fromValue, final Class<T> toValueType) throws IllegalArgumentException {
		return objectMapper.convertValue(fromValue, toValueType);
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public <T extends Collection<E>, E> T convertValue(final Object fromValue,
													   final Class<T> collectionType,
													   final Class<E> elementType) throws IllegalArgumentException {
		final CollectionType colType = objectMapper.getTypeFactory().constructCollectionType(collectionType,
				elementType);

		return (T) objectMapper.convertValue(fromValue, colType);
	}

	private abstract static class ObjectIterable implements Iterable<Object> {

		@Override
		public Iterator<Object> iterator() {
			return new ObjectIterator(getBaseIterator());
		}

		protected abstract Iterator<?> getBaseIterator();

		private static class ObjectIterator implements Iterator<Object> {

			private final Iterator<?> fBaseIterator;

			ObjectIterator(final Iterator<?> baseIterator) {
				fBaseIterator = baseIterator;
			}

			@Override
			public boolean hasNext() {
				return fBaseIterator.hasNext();
			}

			@Override
			public Object next() {
				return fBaseIterator.next();
			}

			@Override
			public void remove() {
				fBaseIterator.remove();
			}
		}
	}

}
