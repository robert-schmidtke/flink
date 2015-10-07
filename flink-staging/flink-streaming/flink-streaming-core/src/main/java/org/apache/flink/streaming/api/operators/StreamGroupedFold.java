/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.streaming.api.state.KVMapCheckpointer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
public class StreamGroupedFold<IN, OUT> extends AbstractUdfStreamOperator<OUT, FoldFunction<IN, OUT>>

		implements OneInputStreamOperator<IN, OUT>, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;

	// Grouped values
	private KeySelector<IN, ?> keySelector;
	private transient OperatorState<HashMap<Object, OUT>> values;

	// Initial value serialization
	private byte[] serializedInitialValue;
	private TypeSerializer<OUT> outTypeSerializer;
	private transient OUT initialValue;

	// Store the typeinfo, create serializer during runtime
	private TypeInformation<Object> keyTypeInformation;

	@SuppressWarnings("unchecked")
	public StreamGroupedFold(FoldFunction<IN, OUT> folder, KeySelector<IN, ?> keySelector,
								OUT initialValue, TypeInformation<IN> inTypeInformation) {
		super(folder);
		this.keySelector = keySelector;
		this.initialValue = initialValue;
		keyTypeInformation = (TypeInformation<Object>) TypeExtractor
				.getKeySelectorTypes(keySelector, inTypeInformation);

	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);

		if (serializedInitialValue == null) {
			throw new RuntimeException("No initial value was serialized for the fold " +
					"operator. Probably the setOutputType method was not called.");
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(serializedInitialValue);
		InputViewDataInputStreamWrapper in = new InputViewDataInputStreamWrapper(
				new DataInputStream(bais)
		);
		initialValue = outTypeSerializer.deserialize(in);

		values = runtimeContext.getOperatorState("flink_internal_fold_values",
				new HashMap<Object, OUT>(), false,
				new KVMapCheckpointer<>(keyTypeInformation.createSerializer(executionConfig),
						outTypeSerializer));
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		Object key = keySelector.getKey(element.getValue());
		OUT value = values.value().get(key);

		if (value != null) {
			OUT folded = userFunction.fold(outTypeSerializer.copy(value), element.getValue());
			values.value().put(key, folded);
			output.collect(element.replace(folded));
		} else {
			OUT first = userFunction.fold(outTypeSerializer.copy(initialValue), element.getValue());
			values.value().put(key, first);
			output.collect(element.replace(first));
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		outTypeSerializer = outTypeInfo.createSerializer(executionConfig);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputViewDataOutputStreamWrapper out = new OutputViewDataOutputStreamWrapper(
				new DataOutputStream(baos)
		);

		try {
			outTypeSerializer.serialize(initialValue, out);
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to serialize initial value of type " +
					initialValue.getClass().getSimpleName() + " of fold operator.", ioe);
		}

		serializedInitialValue = baos.toByteArray();
	}

}
