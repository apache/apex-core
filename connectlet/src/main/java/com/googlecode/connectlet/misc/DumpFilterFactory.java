package com.googlecode.connectlet.misc;

import java.io.File;
import java.io.PrintStream;

import com.googlecode.connectlet.FilterFactory;

/** A factory to create {@link DumpFilter}s. */
public class DumpFilterFactory implements FilterFactory {
	private PrintStream dumpStream = System.out;
	private File dumpFolder = null;
	private boolean dumpText = false, useClientMode = false;

	/**
	 * @param dumpStream - The PrintSteam to dump out.
	 * @return The DumpFilterFactory itself.
	 * @see DumpFilter#setDumpStream(PrintStream)
	 */
	public DumpFilterFactory setDumpStream(PrintStream dumpStream) {
		this.dumpStream = dumpStream;
		return this;
	}

	/**
	 * @param dumpFolder - The folder to dump out.
	 * @return The DumpFilterFactory itself.
	 * @see DumpFilter#setDumpFolder(File)
	 */
	public DumpFilterFactory setDumpFolder(File dumpFolder) {
		this.dumpFolder = dumpFolder;
		return this;
	}

	/**
	 * @param dumpText - Whether dump in text mode (true) or in binary mode (false).
	 * @return The DumpFilterFactory itself.
	 * @see DumpFilter#setDumpText(boolean)
	 */
	public DumpFilterFactory setDumpText(boolean dumpText) {
		this.dumpText = dumpText;
		return this;
	}

	/**
	 * @param useClientMode - Whether dump local address (true) or remote address (false).
	 * @return The DumpFilterFactory itself.
	 * @see DumpFilter#setUseClientMode(boolean)
	 */
	public DumpFilterFactory setUseClientMode(boolean useClientMode) {
		this.useClientMode = useClientMode;
		return this;
	}

	@Override
	public DumpFilter createFilter() {
		return new DumpFilter().setDumpStream(dumpStream).setDumpFolder(dumpFolder).
				setDumpText(dumpText).setUseClientMode(useClientMode);
	}
}