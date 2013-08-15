package com.datatorrent.mapreduce;

import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class TestInputOperator<T> extends BaseOperator implements InputOperator

{

	public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

	transient boolean first;

	transient long windowId;

	boolean blockEndStream = false;

	/**
	 * 
	 * Tuples to be emitted by the operator, with one entry per window.
	 */

	public List<List<T>> testTuples;

	@Override
	public void emitTuples()

	{

		if (testTuples == null || testTuples.isEmpty()) {

			if (blockEndStream) {

				return;

			}

		}

		if (first && !testTuples.isEmpty()) {

			List<T> tuples = testTuples.remove(0);

			for (T t : tuples) {

				output.emit(t);

			}

			first = false;

		}

	}

	@Override
	public void beginWindow(long windowId)

	{

		this.windowId = windowId;

		first = true;

	}

}